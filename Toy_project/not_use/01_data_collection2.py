# 01_data_collection.py
# New Balance 50 products crawling (KREAM) - Drawer-based transactions parser (manual open)
# - persistent Chrome profile (login session reuse)
# - product id collect from brand page (tab=44 shoes)
# - basic + details auto parse
# - transactions: USER opens drawer (거래 및 입찰 내역) + sets (체결 거래) + (과거순), then press Enter
# - crawler scrolls INSIDE the drawer and parses trades
# - checkpoint CSV append per product
# - retry queue

import os, re, csv, time, random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# =========================
# CONFIG
# =========================
BRAND_URL = "https://kream.co.kr/brands/New%20Balance?tab=44"
OUT_CSV = "newbalance_50_2.csv"
TARGET_N = 50

PROFILE_DIR = os.path.abspath("./chrome_profile_kream")

HEADLESS_DEFAULT = False
PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 10
SLEEP_MIN = 0.9
SLEEP_MAX = 1.9

# Golden sizes (team result)
GOLDEN_UNISEX = {"265", "270"}      # 필요하면 275 추가
GOLDEN_WOMEN = {"235", "240", "245"}

# Manual mode (recommended for drawer UI stability)
MANUAL_OPEN_DRAWER_EACH_PRODUCT = True  # 상품마다: 패널 열고/정렬 후 엔터

# =========================
# CSV
# =========================
FIELD_ORDER = [
    "product_id", "한글명", "관심수",
    "모델번호", "발매일", "발매가", "색상",
    "is_womens",
    "Week1_Avg", "Week2_Avg", "Week3_Avg", "Week4_Avg",
    "error"
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

    # mild UA
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
# CHECKPOINT CSV (append)
# =========================
def load_done_ids(out_csv: str) -> Set[str]:
    if not os.path.exists(out_csv):
        return set()
    try:
        df = pd.read_csv(out_csv, dtype={"product_id": str})
        if "product_id" in df.columns:
            return set(df["product_id"].dropna().astype(str).tolist())
    except Exception:
        return set()
    return set()


def append_row(out_csv: str, row: Dict):
    file_exists = os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELD_ORDER)
        if not file_exists:
            w.writeheader()
        safe = {k: row.get(k, None) for k in FIELD_ORDER}
        w.writerow(safe)


# =========================
# BRAND PRODUCT IDS (dynamic scroll)
# =========================
def extract_product_ids_from_html(html: str) -> List[str]:
    # keep order of appearance
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
# TRANSACTIONS (Drawer-based, manual open)
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
    Find scrollable container inside the drawer opened by '거래 및 입찰 내역'.
    """
    # 1) find title
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

    # 2) get a larger container around title
    container = title_el
    for _ in range(4):
        try:
            container = container.find_element(By.XPATH, "./ancestor::*[self::div or self::section][1]")
        except Exception:
            break

    # 3) inside container, find the best scrollable div
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


def scroll_inside_element(driver: webdriver.Chrome, el, max_steps=40, pause=0.55) -> None:
    last_top = -1
    stagnant = 0
    for _ in range(max_steps):
        try:
            top = driver.execute_script("return arguments[0].scrollTop;", el)
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].clientHeight;", el)
            time.sleep(pause)
            new_top = driver.execute_script("return arguments[0].scrollTop;", el)
            if new_top == top or new_top == last_top:
                stagnant += 1
            else:
                stagnant = 0
            last_top = new_top
            if stagnant >= 4:
                break
        except Exception:
            break


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
        if len(t) > 200:
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


def get_kream_transactions_1month_from_drawer(driver: webdriver.Chrome,
                                              release_dt_str: Optional[str],
                                              is_womens: bool) -> Dict:
    today_date = datetime.now().date()

    release_dt = None
    if release_dt_str:
        try:
            release_dt = pd.to_datetime("20" + release_dt_str, format="%Y/%m/%d").date()
        except Exception:
            release_dt = None

    drawer_scroll = find_trade_drawer_scrollable(driver)
    if drawer_scroll is None:
        return {"Week1_Avg": None, "Week2_Avg": None, "Week3_Avg": None, "Week4_Avg": None}

    # Scroll inside drawer to load enough rows
    scroll_inside_element(driver, drawer_scroll, max_steps=45, pause=0.55)

    drawer_html = drawer_scroll.get_attribute("innerHTML") or ""
    trades = extract_trades_from_drawer_html(drawer_html)

    golden = GOLDEN_WOMEN if is_womens else GOLDEN_UNISEX
    tx = []

    for size, price, date_str in trades:
        if size not in golden:
            continue

        trade_date = parse_kream_date(today_date, date_str)
        if not trade_date:
            continue

        if release_dt is None:
            # Oldest sort assumed (user did it)
            release_dt = trade_date

        days_since = (trade_date - release_dt).days
        if days_since < 0:
            continue
        if days_since > 30:
            break

        tx.append({"price": price, "days_since": days_since})

    df = pd.DataFrame(tx)
    if df.empty:
        return {"Week1_Avg": None, "Week2_Avg": None, "Week3_Avg": None, "Week4_Avg": None}

    df["week_idx"] = df["days_since"] // 7
    wk = df.groupby("week_idx")["price"].mean()

    def _get(i):
        return int(wk.get(i)) if i in wk else None

    return {"Week1_Avg": _get(0), "Week2_Avg": _get(1), "Week3_Avg": _get(2), "Week4_Avg": _get(3)}


def manual_prepare_drawer(product_id: str):
    print("\n" + "=" * 78)
    print(f"[MANUAL] product_id={product_id}")
    print("1) 오른쪽에 '거래 및 입찰 내역' 패널(드로어)을 열기")
    print("2) 탭이 '체결 거래'인지 확인")
    print("3) 정렬을 '과거순(오래된 순)'으로 맞추기")
    print("4) 패널 리스트가 보이는 상태에서 엔터")
    print("=" * 78)
    input("👉 준비 완료 후 엔터...")


# =========================
# SINGLE PRODUCT COLLECT
# =========================
def collect_one_product(driver: webdriver.Chrome, product_id: str) -> Dict:
    basic = get_kream_basic_info(driver, product_id)
    name_ko = basic.get("한글명", "Unknown")
    wish = basic.get("관심수", 0)
    is_womens = "(W)" in (name_ko or "")

    details = get_kream_details_auto(driver)

    if MANUAL_OPEN_DRAWER_EACH_PRODUCT:
        manual_prepare_drawer(product_id)

    tx = get_kream_transactions_1month_from_drawer(driver, details.get("발매일"), is_womens)

    return {
        "product_id": str(product_id),
        "한글명": name_ko,
        "관심수": wish,
        "모델번호": details.get("모델번호"),
        "발매일": details.get("발매일"),
        "발매가": details.get("발매가"),
        "색상": details.get("색상"),
        "is_womens": int(is_womens),
        **tx,
        "error": None
    }


# =========================
# MAIN RUN (retry + checkpoint)
# =========================
def backoff_sleep(attempt_round: int):
    t = min(2 ** attempt_round, 20) + random.uniform(0.3, 1.2)
    time.sleep(t)


def run_newbalance_collection(target_n=50,
                              out_csv=OUT_CSV,
                              brand_url=BRAND_URL,
                              profile_dir=PROFILE_DIR,
                              headless=HEADLESS_DEFAULT,
                              retry_rounds=2):
    driver = make_driver(headless=headless, profile_dir=profile_dir)

    try:
        driver.get("https://kream.co.kr")
        print("\n[STEP] 로그인 완료 후 엔터 (프로필로 세션 유지)")
        input("👉 로그인 완료 후 엔터...")

        product_ids = collect_brand_product_ids(driver, brand_url, target_n=target_n)
        print(f"[OK] product_id 수집: {len(product_ids)}개")

        done_ids = load_done_ids(out_csv)
        todo = [pid for pid in product_ids if str(pid) not in done_ids]
        print(f"[INFO] 이미 수집된 id: {len(done_ids)}개, 이번 실행 대상: {len(todo)}개")

        failed = []

        # 1st pass
        for i, pid in enumerate(todo, 1):
            print(f"\n[{i}/{len(todo)}] product_id={pid}")
            try:
                row = collect_one_product(driver, str(pid))
                append_row(out_csv, row)
                human_sleep()
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                print(f"  [FAIL] {pid} -> {msg}")
                failed.append(str(pid))
                append_row(out_csv, {"product_id": str(pid), "error": msg})

        # retry rounds
        for r in range(1, retry_rounds + 1):
            if not failed:
                break

            done_ids = load_done_ids(out_csv)
            failed = [pid for pid in failed if pid not in done_ids]
            if not failed:
                break

            print(f"\n[RETRY ROUND {r}] 재시도 대상: {len(failed)}개")
            next_failed = []

            for pid in failed:
                print(f"  [RETRY {r}] {pid}")
                try:
                    backoff_sleep(r)
                    row = collect_one_product(driver, pid)
                    append_row(out_csv, row)
                    human_sleep()
                except Exception as e:
                    msg = f"{type(e).__name__}: {e}"
                    print(f"    [RETRY FAIL] {pid} -> {msg}")
                    next_failed.append(pid)
                    append_row(out_csv, {"product_id": pid, "error": f"RETRY{r} {msg}"})

            failed = next_failed

        if failed:
            print("\n[WARN] 끝까지 실패한 product_id들:")
            print(failed)

        print(f"\n[DONE] 저장: {out_csv}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    run_newbalance_collection(target_n=50, headless=False, out_csv="newbalance_50_2.csv")
