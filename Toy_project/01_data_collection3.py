# 01_data_collection.py
# New Balance 50 products crawling (KREAM)
# ✅ 최종 전략(요청 반영):
# - 사용자는 "거래 및 입찰 내역" 드로어를 열고, "체결 거래" 탭 + "오래된 순(과거순)"만 맞춘다.
# - 코드는 드로어 맨 위(가장 오래된 거래일) 기준 start_date를 잡고,
#   start_date + 180일까지(윈도우 180일) 거래를 스크롤하며 수집한다.
# - 저장은 골든사이즈(6개) 거래만 저장한다. (종료 판단은 전체 거래 날짜로 수행)
# - 전처리(주차 평균 등)는 수집 이후에 별도 수행.

import os, re, csv, time, random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# =========================
# CONFIG
# =========================
BRAND_URL = "https://kream.co.kr/brands/New%20Balance?tab=44"
TARGET_N = 50

PROFILE_DIR = os.path.abspath("./chrome_profile_kream")
HEADLESS_DEFAULT = False

PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 10
SLEEP_MIN = 0.9
SLEEP_MAX = 1.9

# ✅ Golden sizes (총 6개)
GOLDEN_SIZES: Set[str] = {"235", "240", "245", "260", "265", "270"}

# 수동 설정 후 엔터 -> 자동 크롤링
MANUAL_OPEN_DRAWER_EACH_PRODUCT = True

# CSV outputs
OUT_PRODUCTS_CSV = "newbalance_products.csv"
OUT_TRADES_CSV   = "newbalance_trades.csv"

# 수집 윈도우(일)
WINDOW_DAYS = 180

# Drawer scroll limits
MAX_SCROLL_STEPS = 160
SCROLL_PAUSE = 0.55


# =========================
# CSV SCHEMA
# =========================
PRODUCT_FIELDS = [
    "product_id", "한글명", "관심수",
    "모델번호", "발매일", "발매가", "색상",
    "is_womens",
    "trade_count",
    "window_start", "window_cutoff",
    "status", "error",
    "collected_at"
]

TRADE_FIELDS = [
    "product_id",
    "size", "price",
    "date_str", "trade_date",
    "window_start", "window_cutoff",
    "collected_at"
]


# =========================
# DRIVER / WAIT HELPERS
# =========================
def make_driver(headless: bool, profile_dir: str) -> webdriver.Chrome:
    os.makedirs(profile_dir, exist_ok=True)

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=ko-KR")

    # Persistent profile (login session reuse)
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--profile-directory=Default")

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def _wait(driver: webdriver.Chrome, timeout=WAIT_TIMEOUT) -> WebDriverWait:
    return WebDriverWait(driver, timeout)


def wait_click(driver: webdriver.Chrome, by: By, sel: str, timeout=WAIT_TIMEOUT) -> bool:
    try:
        el = _wait(driver, timeout).until(EC.element_to_be_clickable((by, sel)))
        driver.execute_script("arguments[0].click();", el)
        return True
    except Exception:
        return False


def human_sleep(mult: float = 1.0):
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX) * mult)


# =========================
# CSV IO
# =========================
def load_done_ids(products_csv: str) -> Set[str]:
    """
    products.csv에 기록된 product_id는 다음 실행에서 스킵.
    (OK/FAIL/SKIP 상관없이 '한 번 처리한 id'로 간주)
    """
    if not os.path.exists(products_csv):
        return set()
    try:
        df = pd.read_csv(products_csv, dtype={"product_id": str})
        if "product_id" in df.columns:
            return set(df["product_id"].dropna().astype(str).tolist())
    except Exception:
        return set()
    return set()


def append_row(out_csv: str, row: Dict, field_order: List[str]):
    file_exists = os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=field_order)
        if not file_exists:
            w.writeheader()
        safe = {k: row.get(k, None) for k in field_order}
        w.writerow(safe)


# =========================
# BRAND PRODUCT IDS (dynamic scroll)
# =========================
def extract_product_ids_from_html(html: str) -> List[str]:
    return re.findall(r"/products/(\d+)", html or "")


def collect_brand_product_ids(driver: webdriver.Chrome, brand_url: str, target_n=50, max_scroll_rounds=60) -> List[str]:
    driver.get(brand_url)
    time.sleep(2.0)

    seen: Set[str] = set()
    ordered: List[str] = []
    stagnation = 0
    prev_len = 0

    for _ in range(max_scroll_rounds):
        ids = extract_product_ids_from_html(driver.page_source)
        for pid in ids:
            if pid not in seen:
                seen.add(pid)
                ordered.append(pid)

        if len(ordered) == prev_len:
            stagnation += 1
        else:
            stagnation = 0
            prev_len = len(ordered)

        if len(ordered) >= target_n:
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.9)

        if stagnation >= 8:
            break

    return ordered[:target_n]


# =========================
# BASIC INFO (Korean name + wish count)
# =========================
def parse_wish_count_text(wish_str: str) -> int:
    wish_str = (wish_str or "").strip()
    if not wish_str:
        return 0
    if "만" in wish_str:
        num = re.sub(r"[^0-9.]", "", wish_str)
        return int(float(num) * 10000) if num else 0
    num = re.sub(r"[^0-9]", "", wish_str)
    return int(num) if num else 0


def get_kream_basic_info(driver: webdriver.Chrome, product_id: str) -> Dict:
    url = f"https://kream.co.kr/products/{product_id}"
    driver.get(url)
    time.sleep(1.2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    info = {"한글명": "Unknown", "관심수": 0}

    target_selector = f'[data-sdui-id="product_wish_count/{product_id}"]'
    wish_el = soup.select_one(target_selector) or soup.select_one('[data-sdui-id*="product_wish_count"]')
    if wish_el:
        info["관심수"] = parse_wish_count_text(wish_el.get_text(strip=True))

    for p in soup.find_all("p"):
        style = p.get("style", "")
        if style and "font-size:15" in style and "line-clamp:1" in style:
            info["한글명"] = p.get_text(strip=True)
            break

    return info


# =========================
# DETAILS (auto expand + parse p tags)
# =========================
def expand_details(driver: webdriver.Chrome):
    candidates = [
        (By.XPATH, "//*[contains(text(),'혜택 더보기')]/ancestor::button[1]"),
        (By.XPATH, "//*[contains(text(),'더보기')]/ancestor::button[1]"),
        (By.XPATH, "//*[contains(text(),'상세')]/ancestor::button[1]"),
        (By.XPATH, "//button//*[name()='svg']/ancestor::button[1]"),
    ]
    for by, sel in candidates:
        if wait_click(driver, by, sel, timeout=2):
            time.sleep(0.3)
            break


def get_kream_details_auto(driver: webdriver.Chrome) -> Dict:
    expand_details(driver)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    details = {"모델번호": None, "발매일": None, "발매가": None, "색상": None}
    for p in soup.find_all("p"):
        text = p.get_text(strip=True)
        if not text:
            continue

        if text.startswith("모델번호"):
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                v = parts[1].strip()
                if v and "정보 없음" not in v and v != "-":
                    details["모델번호"] = v

        elif text.startswith("발매일"):
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                v = parts[1].strip()
                m = re.search(r"\d{2}/\d{2}/\d{2}", v)
                if m:
                    details["발매일"] = m.group()

        elif text.startswith("발매가"):
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                v = parts[1].strip()
                num = re.sub(r"[^0-9]", "", v)
                details["발매가"] = int(num) if num else None

        elif text.startswith("색상"):
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                v = parts[1].strip()
                if v and "정보 없음" not in v and v != "-":
                    details["색상"] = v

    return details


# =========================
# TRANSACTIONS (Drawer-based)
# =========================
def parse_kream_date(today_date, d_str: str) -> Optional[datetime.date]:
    d_str = (d_str or "").strip()
    if not d_str:
        return None

    if "분 전" in d_str or "시간 전" in d_str:
        return today_date
    if "일 전" in d_str:
        days_ago = int(re.sub(r"[^0-9]", "", d_str))
        return today_date - timedelta(days=days_ago)

    m = re.search(r"\d{2}/\d{2}/\d{2}", d_str)
    if m:
        return pd.to_datetime("20" + m.group(), format="%Y/%m/%d").date()

    return None


def find_trade_drawer_scrollable(driver: webdriver.Chrome):
    """
    Find a scrollable container inside the drawer opened by '거래 및 입찰 내역'.
    """
    title_xpaths = [
        "//*[contains(text(),'거래 및 입찰 내역')]",
        "//*[contains(text(),'거래') and contains(text(),'입찰') and contains(text(),'내역')]",
    ]
    title_el = None
    for xp in title_xpaths:
        try:
            title_el = _wait(driver, 3).until(EC.presence_of_element_located((By.XPATH, xp)))
            break
        except Exception:
            pass
    if title_el is None:
        return None

    container = title_el
    for _ in range(4):
        try:
            container = container.find_element(By.XPATH, "./ancestor::*[self::div or self::section][1]")
        except Exception:
            break

    scrollables = container.find_elements(By.XPATH, ".//div")
    best = None
    best_sh = 0
    for el in scrollables:
        try:
            sh = driver.execute_script("return arguments[0].scrollHeight;", el)
            ch = driver.execute_script("return arguments[0].clientHeight;", el)
            if sh and ch and sh > ch and sh > best_sh:
                best = el
                best_sh = sh
        except Exception:
            continue

    return best or container


def extract_trades_from_drawer_html(html: str) -> List[Tuple[str, int, str]]:
    """
    Parse rows from drawer HTML by pattern:
    size(3 digits) + price(원) + date(yy/mm/dd or ~일 전)
    """
    soup = BeautifulSoup(html, "html.parser")
    out = []

    for el in soup.find_all(["tr", "li", "div"]):
        t = el.get_text(" ", strip=True)
        if not t:
            continue
        if "원" not in t:
            continue
        if not re.search(r"\b\d{3}\b", t):
            continue
        if not (re.search(r"\d{2}/\d{2}/\d{2}", t) or "일 전" in t or "시간 전" in t or "분 전" in t):
            continue
        if len(t) > 220:
            continue

        m_size = re.search(r"\b(\d{3})\b", t)
        m_price = re.search(r"(\d[\d,]*)\s*원", t)
        m_date = re.search(r"(\d{2}/\d{2}/\d{2}|\d+\s*일 전|\d+\s*시간 전|\d+\s*분 전)", t)

        if m_size and m_price and m_date:
            size = m_size.group(1)
            price = int(m_price.group(1).replace(",", ""))
            date_str = m_date.group(1)
            out.append((size, price, date_str))

    return out


def crawl_golden_window90_from_drawer(driver: webdriver.Chrome,
                                     golden_sizes: Set[str],
                                     window_days: int = 90,
                                     max_steps: int = 160,
                                     pause: float = 0.55) -> Tuple[List[Dict], Optional[datetime.date], Optional[datetime.date]]:
    """
    사용자: 체결거래 + 오래된순(과거순) 세팅
    코드:
      1) 첫 로딩 화면에서 '가장 오래된 거래일(start_date)'을 잡음(= min date)
      2) cutoff = start_date + window_days
      3) 스크롤을 내리며(더 최신으로) latest_seen_date가 cutoff를 넘길 때까지 수집
      4) 저장은 golden_sizes만 저장하되, 종료 판단은 전체 거래 날짜로 함
    반환:
      (golden_trades, start_date, cutoff_date)
    """
    today = datetime.now().date()
    el = find_trade_drawer_scrollable(driver)
    if el is None:
        return [], None, None

    seen = set()  # (size, price, date_str)
    golden_out: List[Dict] = []

    start_date: Optional[datetime.date] = None
    cutoff_date: Optional[datetime.date] = None
    latest_seen: Optional[datetime.date] = None

    stagnant = 0
    last_seen_count = 0

    def parse_once() -> None:
        nonlocal start_date, cutoff_date, latest_seen

        html = el.get_attribute("innerHTML") or ""
        rows = extract_trades_from_drawer_html(html)

        min_d = None
        max_d = None

        for size, price, date_str in rows:
            key = (size, price, date_str)
            if key in seen:
                continue
            seen.add(key)

            d = parse_kream_date(today, date_str)

            if d:
                min_d = d if (min_d is None or d < min_d) else min_d
                max_d = d if (max_d is None or d > max_d) else max_d

            if size in golden_sizes:
                golden_out.append({
                    "size": size,
                    "price": price,
                    "date_str": date_str,
                    "trade_date": str(d) if d else None
                })

        # start_date: 처음 화면에서 관측되는 가장 오래된 날짜(오래된순이면 min이 start)
        if start_date is None and min_d is not None:
            start_date = min_d
            cutoff_date = start_date + timedelta(days=window_days)

        # latest_seen: 전체 거래 중 가장 최신 날짜를 계속 갱신
        if max_d is not None:
            latest_seen = max_d if (latest_seen is None or max_d > latest_seen) else latest_seen

    # 첫 화면 로딩 지연 대응: start_date 잡힐 때까지 여러 번 파싱
    for _ in range(6):
        parse_once()
        if start_date is not None:
            break
        time.sleep(0.6)

    if start_date is None:
        # 날짜 파싱을 못하면: 그래도 최대한 스크롤해서 골든만 긁고 종료
        for _ in range(45):
            try:
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;", el)
            except Exception:
                break
            time.sleep(pause)
            parse_once()
        return golden_out, None, None

    # cutoff 넘길 때까지 스크롤
    for _ in range(max_steps):
        parse_once()

        if cutoff_date is not None and latest_seen is not None and latest_seen >= cutoff_date:
            # 누락 방지: 2회 여유 스크롤
            for _ in range(2):
                try:
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;", el)
                except Exception:
                    break
                time.sleep(pause)
                parse_once()
            break

        # 스크롤 진행
        try:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;", el)
        except Exception:
            break
        time.sleep(pause)

        # 정체 감지(새 row가 더 안 늘어남)
        if len(seen) == last_seen_count:
            stagnant += 1
        else:
            stagnant = 0
            last_seen_count = len(seen)
        if stagnant >= 6:
            break

    return golden_out, start_date, cutoff_date


# =========================
# MANUAL STEP
# =========================
def manual_prepare_drawer(product_id: str):
    print("\n" + "=" * 88)
    print(f"[MANUAL] product_id={product_id}")
    print("✅ 아래를 '사용자'가 직접 수행하세요:")
    print("1) 오른쪽에 '거래 및 입찰 내역' 드로어(패널) 열기")
    print("2) 탭을 '체결 거래'로 맞추기")
    print("3) 정렬을 '오래된 순(과거순)'으로 바꾸기  ← 중요")
    print("4) 리스트(거래내역)가 보이게 둔 상태에서 Enter")
    print("=" * 88)
    input("👉 준비 완료 후 Enter...")


# =========================
# SINGLE PRODUCT COLLECT
# =========================
def collect_one_product(driver: webdriver.Chrome, product_id: str) -> Tuple[Dict, List[Dict], Optional[datetime.date], Optional[datetime.date]]:
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    basic = get_kream_basic_info(driver, product_id)
    name_ko = basic.get("한글명", "Unknown")
    wish = basic.get("관심수", 0)

    details = get_kream_details_auto(driver)

    if MANUAL_OPEN_DRAWER_EACH_PRODUCT:
        manual_prepare_drawer(product_id)

    trades_golden, start_date, cutoff_date = crawl_golden_window90_from_drawer(
        driver,
        golden_sizes=GOLDEN_SIZES,
        window_days=WINDOW_DAYS,
        max_steps=MAX_SCROLL_STEPS,
        pause=SCROLL_PAUSE
    )

    status = "OK" if trades_golden else "FAIL"
    err = None if trades_golden else "NO_GOLDEN_TRADES_PARSED"

    product_row = {
        "product_id": str(product_id),
        "한글명": name_ko,
        "관심수": wish,
        "모델번호": details.get("모델번호"),
        "발매일": details.get("발매일"),
        "발매가": details.get("발매가"),
        "색상": details.get("색상"),
        "is_womens": int("(W)" in (name_ko or "")),
        "trade_count": len(trades_golden),
        "window_start": str(start_date) if start_date else None,
        "window_cutoff": str(cutoff_date) if cutoff_date else None,
        "status": status,
        "error": err,
        "collected_at": collected_at
    }

    return product_row, trades_golden, start_date, cutoff_date


# =========================
# MAIN RUN
# =========================
def backoff_sleep(attempt_round: int):
    t = min(2 ** attempt_round, 20) + random.uniform(0.3, 1.2)
    time.sleep(t)


def run_newbalance_collection(target_n=50,
                              products_csv=OUT_PRODUCTS_CSV,
                              trades_csv=OUT_TRADES_CSV,
                              brand_url=BRAND_URL,
                              profile_dir=PROFILE_DIR,
                              headless=HEADLESS_DEFAULT,
                              retry_rounds=1):
    driver = make_driver(headless=headless, profile_dir=profile_dir)

    try:
        driver.get("https://kream.co.kr")
        print("\n[STEP] 로그인 완료 후 Enter (프로필로 세션 유지)")
        input("👉 로그인 완료 후 Enter...")

        product_ids = collect_brand_product_ids(driver, brand_url, target_n=target_n)
        print(f"[OK] product_id 수집: {len(product_ids)}개")

        done_ids = load_done_ids(products_csv)
        todo = [pid for pid in product_ids if str(pid) not in done_ids]
        print(f"[INFO] 이미 처리된 id: {len(done_ids)}개, 이번 실행 대상: {len(todo)}개")

        failed = []

        for i, pid in enumerate(todo, 1):
            print(f"\n[{i}/{len(todo)}] product_id={pid}")
            try:
                product_row, trades, start_date, cutoff_date = collect_one_product(driver, str(pid))

                append_row(products_csv, product_row, PRODUCT_FIELDS)

                if trades:
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ws = str(start_date) if start_date else None
                    wc = str(cutoff_date) if cutoff_date else None

                    for t in trades:
                        append_row(trades_csv, {
                            "product_id": str(pid),
                            "size": t.get("size"),
                            "price": t.get("price"),
                            "date_str": t.get("date_str"),
                            "trade_date": t.get("trade_date"),
                            "window_start": ws,
                            "window_cutoff": wc,
                            "collected_at": now_str
                        }, TRADE_FIELDS)

                human_sleep()

            except KeyboardInterrupt:
                print("\n[STOP] 사용자 종료. 현재까지 저장된 CSV는 유지됩니다.")
                break
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                print(f"  [FAIL] {pid} -> {msg}")
                failed.append(str(pid))
                append_row(products_csv, {
                    "product_id": str(pid),
                    "status": "FAIL",
                    "error": msg,
                    "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }, PRODUCT_FIELDS)

        # simple retry (optional)
        for r in range(1, retry_rounds + 1):
            if not failed:
                break

            # products.csv에 기록된 id는 제외
            done_ids = load_done_ids(products_csv)
            failed = [pid for pid in failed if pid not in done_ids]
            if not failed:
                break

            print(f"\n[RETRY ROUND {r}] 재시도 대상: {len(failed)}개")
            next_failed = []

            for pid in failed:
                print(f"  [RETRY {r}] {pid}")
                try:
                    backoff_sleep(r)
                    product_row, trades, start_date, cutoff_date = collect_one_product(driver, pid)

                    append_row(products_csv, product_row, PRODUCT_FIELDS)

                    if trades:
                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ws = str(start_date) if start_date else None
                        wc = str(cutoff_date) if cutoff_date else None

                        for t in trades:
                            append_row(trades_csv, {
                                "product_id": str(pid),
                                "size": t.get("size"),
                                "price": t.get("price"),
                                "date_str": t.get("date_str"),
                                "trade_date": t.get("trade_date"),
                                "window_start": ws,
                                "window_cutoff": wc,
                                "collected_at": now_str
                            }, TRADE_FIELDS)

                    human_sleep()

                except KeyboardInterrupt:
                    print("\n[STOP] 사용자 종료.")
                    next_failed = []
                    break
                except Exception as e:
                    msg = f"{type(e).__name__}: {e}"
                    print(f"    [RETRY FAIL] {pid} -> {msg}")
                    next_failed.append(pid)
                    append_row(products_csv, {
                        "product_id": str(pid),
                        "status": "FAIL",
                        "error": f"RETRY{r} {msg}",
                        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }, PRODUCT_FIELDS)

            failed = next_failed

        if failed:
            print("\n[WARN] 끝까지 실패한 product_id들:")
            print(failed)

        print(f"\n[DONE] 저장:")
        print(f" - products: {products_csv}")
        print(f" - trades   : {trades_csv}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    run_newbalance_collection(
        target_n=50,
        headless=False,
        products_csv=OUT_PRODUCTS_CSV,
        trades_csv=OUT_TRADES_CSV,
    )
