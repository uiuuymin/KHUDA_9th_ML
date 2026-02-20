# 01_data_collection_txt.py
# Nike (KREAM) product_id TXT 리스트 기반 크롤러
#
# ✅ 운영 방식
# - product_ids.txt (한 줄에 product_id 하나)만 읽어서 해당 제품만 크롤링
# - 로그인은 크롬 프로필로 유지
# - 각 제품에서 사용자가 직접: "거래 및 입찰 내역" 드로어 열기 + "체결 거래" + "오래된 순" 세팅
# - 코드는 드로어에서 거래내역을 최대 2000개까지(전체 사이즈) 스크롤하며 수집
# - 평균/골든사이즈 필터링/라벨링은 전처리에서 수행
#
# 출력:
# - nike_products.csv
# - nike_trades.csv
#
# ⚠️ 권장:
# - 새 실험/런마다 OUT_PRODUCTS_CSV/OUT_TRADES_CSV 파일명을 바꾸거나 기존 파일 삭제 후 시작

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
PROFILE_DIR = os.path.abspath("./chrome_profile_kream")
HEADLESS_DEFAULT = False

PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 10
SLEEP_MIN = 0.9
SLEEP_MAX = 1.9

# ✅ TXT 리스트 입력
PRODUCT_IDS_TXT = "product_ids.txt"

# ✅ 거래내역 최대 수집 개수
MAX_TRADES_PER_PRODUCT = 2000

# 수동 설정 후 엔터 -> 자동 크롤링 (거래 드로어)
MANUAL_OPEN_DRAWER_EACH_PRODUCT = True

# Drawer scroll limits
DRAWER_MAX_SCROLL_STEPS = 2000
DRAWER_SCROLL_PAUSE = 0.55

# CSV outputs
OUT_PRODUCTS_CSV = "01_nike_products.csv"
OUT_TRADES_CSV   = "01_nike_trades.csv"


# =========================
# CSV SCHEMA
# =========================
PRODUCT_FIELDS = [
    "product_id",
    "name",
    "모델번호",
    "wish_count",
    "발매일",
    "발매가",
    "색상",
    "한정판 여부",         # 수기(빈칸)
    "국내발매 여부",       # 수기(빈칸)
    "콜라보 여부",         # 수기(빈칸)
    "구글트랜드 분석 결과",  # 추후(빈칸)
    "라벨링",             # 추후(빈칸)
    # 메타
    "trade_count",
    "status",
    "error",
    "collected_at",
]

TRADE_FIELDS = [
    "product_id",
    "size",
    "price",
    "date_str",
    "trade_date",
    "collected_at",
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
# TXT IO
# =========================
def load_product_ids_from_txt(path: str) -> List[str]:
    """
    product_ids.txt 형식:
      - 한 줄에 하나: 12345
      - 공백/탭/쉼표 섞여 있어도 파싱
      - '#'로 시작하는 줄은 주석으로 무시
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"TXT not found: {path}")

    ids: List[str] = []
    seen: Set[str] = set()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            # 쉼표/공백/탭으로 분리 + 숫자만 추출
            parts = re.split(r"[,\s]+", s)
            for p in parts:
                p = p.strip()
                if not p:
                    continue
                m = re.search(r"(\d+)", p)
                if not m:
                    continue
                pid = m.group(1)
                if pid not in seen:
                    seen.add(pid)
                    ids.append(pid)

    return ids


# =========================
# CSV IO
# =========================
def load_done_ids(products_csv: str) -> Set[str]:
    """
    products.csv에 기록된 product_id는 다음 실행에서 스킵.
    (OK/FAIL 상관없이 '한 번 처리한 id'로 간주)
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
# BASIC INFO (name + wish count)
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

    info = {"name": "Unknown", "wish_count": 0}

    # wish count
    target_selector = f'[data-sdui-id="product_wish_count/{product_id}"]'
    wish_el = soup.select_one(target_selector) or soup.select_one('[data-sdui-id*="product_wish_count"]')
    if wish_el:
        info["wish_count"] = parse_wish_count_text(wish_el.get_text(strip=True))

    # name (한글명 위주)
    for p in soup.find_all("p"):
        style = p.get("style", "")
        if style and "font-size:15" in style and "line-clamp:1" in style:
            info["name"] = p.get_text(strip=True)
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
                m1 = re.search(r"\d{2}/\d{2}/\d{2}", v)
                m2 = re.search(r"\d{4}-\d{2}-\d{2}", v)
                if m1:
                    details["발매일"] = m1.group()
                elif m2:
                    details["발매일"] = m2.group()

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

    m2 = re.search(r"\d{4}-\d{2}-\d{2}", d_str)
    if m2:
        return pd.to_datetime(m2.group(), format="%Y-%m-%d").date()

    return None


def find_trade_drawer_scrollable(driver: webdriver.Chrome):
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
    soup = BeautifulSoup(html, "html.parser")
    out = []

    for el in soup.find_all(["tr", "li", "div"]):
        t = el.get_text(" ", strip=True)
        if not t:
            continue
        if "원" not in t:
            continue
        # ✅ 사이즈는 3자리 숫자만 뽑되, '240(US5)' 같은 형태도 안전하게 처리
        m_size = re.search(r"(\d{3})", t)  # <- \b 제거(더 안전)
        if not m_size:
            continue
        if not (re.search(r"\d{2}/\d{2}/\d{2}", t) or re.search(r"\d{4}-\d{2}-\d{2}", t)
                or "일 전" in t or "시간 전" in t or "분 전" in t):
            continue
        if len(t) > 240:
            continue

        m_price = re.search(r"(\d[\d,]*)\s*원", t)
        m_date = re.search(r"(\d{2}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2}|\d+\s*일 전|\d+\s*시간 전|\d+\s*분 전)", t)

        if m_price and m_date:
            size = m_size.group(1)
            price = int(m_price.group(1).replace(",", ""))
            date_str = m_date.group(1)
            out.append((size, price, date_str))

    return out


def crawl_trades_max2000_from_drawer(driver: webdriver.Chrome,
                                    max_trades: int = 2000,
                                    max_steps: int = 2000,
                                    pause: float = 0.55) -> List[Dict]:
    today = datetime.now().date()
    el = find_trade_drawer_scrollable(driver)
    if el is None:
        return []

    seen = set()  # (size, price, date_str)
    out: List[Dict] = []

    def parse_once() -> int:
        html = el.get_attribute("innerHTML") or ""
        rows = extract_trades_from_drawer_html(html)

        added = 0
        for size, price, date_str in rows:
            key = (size, price, date_str)
            if key in seen:
                continue
            seen.add(key)
            d = parse_kream_date(today, date_str)
            out.append({
                "size": size,
                "price": price,
                "date_str": date_str,
                "trade_date": str(d) if d else None
            })
            added += 1
            if len(out) >= max_trades:
                break
        return added

    def scroll_step(jump_mult: float = 2.2) -> bool:
        try:
            top_before = driver.execute_script("return arguments[0].scrollTop;", el) or 0
            ch = driver.execute_script("return arguments[0].clientHeight;", el) or 0
            inc = int(max(1, ch * jump_mult))
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[1];", el, inc)
            time.sleep(pause)
            top_after = driver.execute_script("return arguments[0].scrollTop;", el) or 0
            return top_after != top_before
        except Exception:
            return False

    def nudge():
        try:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 30;", el)
            time.sleep(0.2)
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop - 30;", el)
            time.sleep(0.25)
        except Exception:
            pass

    # initial parse
    for _ in range(8):
        parse_once()
        if len(out) > 0:
            break
        time.sleep(0.5)

    if len(out) >= max_trades:
        return out[:max_trades]

    stuck = 0
    no_add_rounds = 0

    for _ in range(max_steps):
        added = parse_once()
        if len(out) >= max_trades:
            for _ in range(2):
                scroll_step(jump_mult=1.8)
                parse_once()
                if len(out) >= max_trades:
                    break
            break

        if added == 0:
            no_add_rounds += 1
        else:
            no_add_rounds = 0

        moved = scroll_step(jump_mult=2.2)
        if not moved:
            stuck += 1
            nudge()
            scroll_step(jump_mult=1.4)
        else:
            stuck = 0

        if stuck >= 25 or no_add_rounds >= 10:
            for _ in range(6):
                nudge()
                scroll_step(jump_mult=1.2)
                if parse_once() > 0:
                    no_add_rounds = 0
                if len(out) >= max_trades:
                    break
            break

    return out[:max_trades]


# =========================
# MANUAL STEP (Drawer)
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
def collect_one_product(driver: webdriver.Chrome, product_id: str) -> Tuple[Dict, List[Dict]]:
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    basic = get_kream_basic_info(driver, product_id)
    name = basic.get("name", "Unknown")
    wish = basic.get("wish_count", 0)

    details = get_kream_details_auto(driver)

    manual_blank = {
        "한정판 여부": None,
        "국내발매 여부": None,
        "콜라보 여부": None,
        "구글트랜드 분석 결과": None,
        "라벨링": None,
    }

    if MANUAL_OPEN_DRAWER_EACH_PRODUCT:
        manual_prepare_drawer(product_id)

    trades = crawl_trades_max2000_from_drawer(
        driver,
        max_trades=MAX_TRADES_PER_PRODUCT,
        max_steps=DRAWER_MAX_SCROLL_STEPS,
        pause=DRAWER_SCROLL_PAUSE
    )

    status = "OK" if trades else "FAIL"
    err = None if trades else "NO_TRADES_PARSED"

    product_row = {
        "product_id": str(product_id),
        "name": name,
        "모델번호": details.get("모델번호"),
        "wish_count": wish,
        "발매일": details.get("발매일"),
        "발매가": details.get("발매가"),
        "색상": details.get("색상"),
        **manual_blank,
        "trade_count": int(len(trades)),
        "status": status,
        "error": err,
        "collected_at": collected_at,
    }

    return product_row, trades


# =========================
# MAIN RUN
# =========================
def backoff_sleep(attempt_round: int):
    t = min(2 ** attempt_round, 20) + random.uniform(0.3, 1.2)
    time.sleep(t)


def run_txt_collection(product_ids_txt=PRODUCT_IDS_TXT,
                       products_csv=OUT_PRODUCTS_CSV,
                       trades_csv=OUT_TRADES_CSV,
                       profile_dir=PROFILE_DIR,
                       headless=HEADLESS_DEFAULT,
                       retry_rounds=1):
    driver = make_driver(headless=headless, profile_dir=profile_dir)

    try:
        driver.get("https://kream.co.kr")
        print("\n[STEP] 로그인 완료 후 Enter (프로필로 세션 유지)")
        input("👉 로그인 완료 후 Enter...")

        product_ids = load_product_ids_from_txt(product_ids_txt)
        print(f"[OK] TXT에서 product_id 로드: {len(product_ids)}개")
        print(product_ids)

        done_ids = load_done_ids(products_csv)
        todo = [pid for pid in product_ids if str(pid) not in done_ids]
        print(f"[INFO] 이미 처리된 id: {len(done_ids)}개, 이번 실행 대상: {len(todo)}개")

        failed: List[str] = []

        for i, pid in enumerate(todo, 1):
            print(f"\n[{i}/{len(todo)}] product_id={pid}")
            try:
                product_row, trades = collect_one_product(driver, str(pid))

                append_row(products_csv, product_row, PRODUCT_FIELDS)

                if trades:
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    for t in trades:
                        append_row(trades_csv, {
                            "product_id": str(pid),
                            "size": t.get("size"),
                            "price": t.get("price"),
                            "date_str": t.get("date_str"),
                            "trade_date": t.get("trade_date"),
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
                    "name": None,
                    "모델번호": None,
                    "wish_count": None,
                    "발매일": None,
                    "발매가": None,
                    "색상": None,
                    "한정판 여부": None,
                    "국내발매 여부": None,
                    "콜라보 여부": None,
                    "구글트랜드 분석 결과": None,
                    "라벨링": None,
                    "trade_count": 0,
                    "status": "FAIL",
                    "error": msg,
                    "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }, PRODUCT_FIELDS)

        # Retry
        for r in range(1, retry_rounds + 1):
            if not failed:
                break

            done_ids = load_done_ids(products_csv)
            failed = [pid for pid in failed if pid not in done_ids]
            if not failed:
                break

            print(f"\n[RETRY ROUND {r}] 재시도 대상: {len(failed)}개")
            next_failed: List[str] = []

            for pid in failed:
                print(f"  [RETRY {r}] {pid}")
                try:
                    backoff_sleep(r)
                    product_row, trades = collect_one_product(driver, pid)

                    append_row(products_csv, product_row, PRODUCT_FIELDS)

                    if trades:
                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        for t in trades:
                            append_row(trades_csv, {
                                "product_id": str(pid),
                                "size": t.get("size"),
                                "price": t.get("price"),
                                "date_str": t.get("date_str"),
                                "trade_date": t.get("trade_date"),
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
                        "product_id": pid,
                        "name": None,
                        "모델번호": None,
                        "wish_count": None,
                        "발매일": None,
                        "발매가": None,
                        "색상": None,
                        "한정판 여부": None,
                        "국내발매 여부": None,
                        "콜라보 여부": None,
                        "구글트랜드 분석 결과": None,
                        "라벨링": None,
                        "trade_count": 0,
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
    run_txt_collection(
        product_ids_txt=PRODUCT_IDS_TXT,
        headless=False,
        products_csv=OUT_PRODUCTS_CSV,
        trades_csv=OUT_TRADES_CSV,
        retry_rounds=1,
    )
