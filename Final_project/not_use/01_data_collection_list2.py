# 01_data_collection_2.py
# Semi-auto KREAM transaction parsing:
# - You manually open "거래/체결 내역" + set "과거순(오래된 순)" on each product page
# - Then press Enter in terminal -> script auto scrolls and parses (30 days window), saves weekly avg
# - Uses fixed product_id list (nb_ids.txt or nb_ids.csv)
# - Checkpoint append to CSV per product + retry queue

import os, re, csv, time, random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# =========================
# CONFIG
# =========================
ID_TXT = "product_id_popularity.txt"     # one id per line
ID_CSV = "product_id_popularity.csv"     # column: product_id

OUT_CSV = "newbalance_50_list2.csv"
PROFILE_DIR = os.path.abspath("./chrome_profile_kream")  # login session reuse

HEADLESS = False          # must be False for manual steps
PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 10

SLEEP_MIN = 0.7
SLEEP_MAX = 1.5

# Golden sizes (your team results)
GOLDEN_UNISEX = {"265", "270"}
GOLDEN_WOMEN = {"235", "240", "245"}

FIELD_ORDER = [
    "product_id", "한글명", "관심수",
    "모델번호", "발매일", "발매가", "색상",
    "is_womens",
    "Week1_Avg", "Week2_Avg", "Week3_Avg", "Week4_Avg",
    "n_trades_used",
    "error"
]


# =========================
# DRIVER / HELPERS
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

    # persistent session
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--profile-directory=Default")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def human_sleep(mult: float = 1.0):
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX) * mult)


def wwait(driver, timeout=WAIT_TIMEOUT):
    return WebDriverWait(driver, timeout)


def try_click_any(driver, candidates: List[Tuple[By, str]], timeout_each=2) -> bool:
    for by, sel in candidates:
        try:
            el = wwait(driver, timeout_each).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            continue
    return False


# =========================
# OVERLAY GUARDS (search overlay)
# =========================
def close_search_overlay(driver):
    # ESC spam
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).send_keys(Keys.ESCAPE).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.2)
    except Exception:
        pass

    # close X candidates
    close_candidates = [
        (By.XPATH, "//*[@aria-label='닫기']/ancestor::button[1]"),
        (By.XPATH, "//*[@aria-label='close']/ancestor::button[1]"),
        (By.XPATH, "//button[contains(@class,'close') or contains(@class,'btn_close')]"),
        (By.XPATH, "//*[text()='닫기']/ancestor::button[1]"),
        (By.XPATH, "//div[contains(@class,'layer') or contains(@class,'overlay') or contains(@class,'modal')]"
                   "//button//*[name()='svg']/ancestor::button[1]"),
    ]
    try_click_any(driver, close_candidates, timeout_each=1)
    time.sleep(0.2)


def ensure_on_product_page(driver, product_id: str):
    url = (driver.current_url or "")
    if f"/products/{product_id}" not in url:
        driver.get(f"https://kream.co.kr/products/{product_id}")
        time.sleep(1.0)
    close_search_overlay(driver)


# =========================
# CHECKPOINT CSV
# =========================
def load_done_ids(out_csv: str) -> Set[str]:
    if not os.path.exists(out_csv):
        return set()
    try:
        df = pd.read_csv(out_csv, dtype={"product_id": str})
        if "product_id" in df.columns:
            return set(df["product_id"].dropna().astype(str).tolist())
    except Exception:
        pass
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
# LOAD IDS
# =========================
def load_product_ids() -> List[str]:
    if os.path.exists(ID_TXT):
        ids = []
        with open(ID_TXT, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                # allow URLs pasted: keep digits only
                s = re.sub(r"[^0-9]", "", s)
                if s:
                    ids.append(s)
        return ids

    if os.path.exists(ID_CSV):
        df = pd.read_csv(ID_CSV, dtype=str)
        if "product_id" not in df.columns:
            raise ValueError("nb_ids.csv에는 product_id 컬럼이 필요합니다.")
        return df["product_id"].dropna().astype(str).tolist()

    raise FileNotFoundError("nb_ids.txt 또는 nb_ids.csv 파일이 필요합니다.")


# =========================
# BASIC / DETAILS (same idea as notebook)
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


def get_kream_basic_info(driver, product_id: str) -> Dict:
    ensure_on_product_page(driver, product_id)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    info = {"한글명": "Unknown", "관심수": 0}

    # wish count
    target_selector = f'[data-sdui-id="product_wish_count/{product_id}"]'
    wish_el = soup.select_one(target_selector) or soup.select_one('[data-sdui-id*="product_wish_count"]')
    if wish_el:
        info["관심수"] = parse_wish_count_text(wish_el.get_text(strip=True))

    # Korean name (style heuristic)
    for p in soup.find_all("p"):
        style = p.get("style", "")
        if style and "font-size:15" in style and "line-clamp:1" in style:
            info["한글명"] = p.get_text(strip=True)
            break

    return info


def expand_details(driver):
    close_search_overlay(driver)
    candidates = [
        (By.XPATH, "//*[contains(text(),'혜택 더보기')]/ancestor::button[1]"),
        (By.XPATH, "//*[contains(text(),'더보기')]/ancestor::button[1]"),
        (By.XPATH, "//*[contains(text(),'상세')]/ancestor::button[1]"),
    ]
    try_click_any(driver, candidates, timeout_each=2)
    time.sleep(0.3)


def get_kream_details_auto(driver, product_id: str) -> Dict:
    ensure_on_product_page(driver, product_id)
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
# TRANSACTION PARSING (TEXT-BASED, robust to CSS changes)
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


def extract_trade_rows_text_based(html: str) -> List[Tuple[str, int, str]]:
    """
    Find trade-like blocks by text pattern:
    - contains size (3 digits)
    - contains price with '원'
    - contains date (yy/mm/dd or 'n일 전' etc.)
    Returns list of (size, price, date_str)
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for el in soup.find_all(["div", "li", "section"]):
        t = el.get_text(" ", strip=True)
        if "원" not in t:
            continue
        if not re.search(r"\b\d{3}\b", t):
            continue
        if not (re.search(r"\d{2}/\d{2}/\d{2}", t) or "일 전" in t or "시간 전" in t or "분 전" in t):
            continue
        if len(t) > 140:
            continue
        candidates.append(t)

    rows = []
    for t in candidates:
        m_size = re.search(r"\b(\d{3})\b", t)
        m_price = re.search(r"(\d[\d,]*)\s*원", t)
        m_date = re.search(r"(\d{2}/\d{2}/\d{2}|(\d+)\s*일 전|(\d+)\s*시간 전|(\d+)\s*분 전)", t)
        if not (m_size and m_price and m_date):
            continue
        size = m_size.group(1)
        price = int(m_price.group(1).replace(",", ""))
        date_str = m_date.group(1)
        rows.append((size, price, date_str))

    return rows


def auto_scroll_some(driver, rounds=10):
    # scroll several times; trade list usually loads progressively
    for _ in range(rounds):
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
        time.sleep(0.85)


def auto_scroll_until_one_month(driver, release_dt: Optional[datetime.date], today_date: datetime.date,
                                max_rounds=80) -> None:
    """
    Keep scrolling while new trade rows appear.
    If release_dt exists, stop when we see trades older than release_dt + 30 days.
    """
    last_n = 0
    stagnant = 0

    for _ in range(max_rounds):
        trades = extract_trade_rows_text_based(driver.page_source)

        if len(trades) <= last_n:
            stagnant += 1
        else:
            stagnant = 0
            last_n = len(trades)

        if release_dt and trades:
            # look at some "older side" candidates; since user set "과거순", earlier trades appear first
            # but UI isn't always perfect—still, this gives early stop signal sometimes.
            for size, price, dstr in trades[-15:]:
                d = parse_kream_date(today_date, dstr)
                if d and (d - release_dt).days > 30:
                    return

        if stagnant >= 6:
            return

        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
        time.sleep(0.85)


def get_transactions_semi_auto(driver, product_id: str, release_dt_str: Optional[str], is_womens: bool) -> Dict:
    """
    Semi-auto:
    - You must open 거래/체결 내역 tab and set "과거순"
    - Then press Enter, script will scroll+parse.
    """
    today_date = datetime.now().date()

    release_dt = None
    if release_dt_str:
        try:
            release_dt = pd.to_datetime("20" + release_dt_str, format="%Y/%m/%d").date()
        except Exception:
            release_dt = None

    ensure_on_product_page(driver, product_id)

    print("\n[MANUAL STEP]")
    print("  1) 화면에서 '거래 내역/체결 내역' 탭을 열고")
    print("  2) 정렬을 '과거순(오래된 순)'으로 바꾼 뒤")
    print("  3) 거래 리스트가 보이게 해주세요.")
    input("👉 준비 완료 후 엔터...")

    # scroll and parse
    auto_scroll_until_one_month(driver, release_dt, today_date, max_rounds=90)
    trades = extract_trade_rows_text_based(driver.page_source)

    golden = GOLDEN_WOMEN if is_womens else GOLDEN_UNISEX

    tx = []
    for size, price, date_str in trades:
        if size not in golden:
            continue
        trade_date = parse_kream_date(today_date, date_str)
        if not trade_date:
            continue

        # if release unknown, use first trade_date (assumes user set oldest sort)
        if release_dt is None:
            release_dt = trade_date

        days_since = (trade_date - release_dt).days
        if days_since < 0:
            continue
        if days_since > 30:
            # if oldest sort is correct, we can break early
            break

        tx.append({"price": price, "days_since": days_since})

    df = pd.DataFrame(tx)
    if df.empty:
        return {"Week1_Avg": None, "Week2_Avg": None, "Week3_Avg": None, "Week4_Avg": None, "n_trades_used": 0}

    df["week_idx"] = df["days_since"] // 7
    wk = df.groupby("week_idx")["price"].mean()

    def _get(i):
        return int(wk.get(i)) if i in wk else None

    return {
        "Week1_Avg": _get(0),
        "Week2_Avg": _get(1),
        "Week3_Avg": _get(2),
        "Week4_Avg": _get(3),
        "n_trades_used": int(len(df))
    }


# =========================
# SINGLE PRODUCT COLLECT
# =========================
def collect_one_product(driver, product_id: str) -> Dict:
    driver.get(f"https://kream.co.kr/products/{product_id}")
    time.sleep(1.0)
    close_search_overlay(driver)

    basic = get_kream_basic_info(driver, product_id)
    name_ko = basic.get("한글명", "Unknown")
    wish = basic.get("관심수", 0)

    is_womens = "(W)" in (name_ko or "")

    details = get_kream_details_auto(driver, product_id)
    tx = get_transactions_semi_auto(driver, product_id, details.get("발매일"), is_womens)

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
        "error": None,
    }


# =========================
# MAIN
# =========================
def backoff_sleep(round_i: int):
    # 2,4,8.. + jitter (capped)
    t = min(2 ** round_i, 20) + random.uniform(0.2, 1.0)
    time.sleep(t)


def main():
    if HEADLESS:
        raise ValueError("반자동(수동 조작) 방식은 HEADLESS=False여야 합니다.")

    product_ids = load_product_ids()
    print(f"[INFO] ID 로드: {len(product_ids)}개")

    done_ids = load_done_ids(OUT_CSV)
    todo = [pid for pid in product_ids if pid not in done_ids]
    print(f"[INFO] 이미 수집된 id: {len(done_ids)}개, 이번 실행 대상: {len(todo)}개")

    driver = make_driver(headless=HEADLESS, profile_dir=PROFILE_DIR)

    try:
        # login once
        driver.get("https://kream.co.kr")
        print("\n[STEP] 처음 실행이면 로그인 해주세요. (PROFILE_DIR로 세션 유지)")
        input("👉 로그인 완료 후 엔터...")

        failed: List[str] = []

        # PASS 1
        for i, pid in enumerate(todo, 1):
            print(f"\n==============================")
            print(f"[{i}/{len(todo)}] product_id={pid}")
            print(f"==============================")

            try:
                row = collect_one_product(driver, pid)
                append_row(OUT_CSV, row)
                print(f"[OK] saved: {pid} | trades_used={row.get('n_trades_used')}")
                human_sleep()

            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                print(f"[FAIL] {pid} -> {msg}")
                failed.append(pid)
                append_row(OUT_CSV, {"product_id": pid, "error": msg})
                human_sleep(1.5)

        # RETRY QUEUE (semi-auto도 재시도 가능)
        retry_rounds = 2
        for r in range(1, retry_rounds + 1):
            if not failed:
                break

            done_ids = load_done_ids(OUT_CSV)
            failed = [pid for pid in failed if pid not in done_ids]
            if not failed:
                break

            print(f"\n[RETRY ROUND {r}] 대상: {len(failed)}개")
            next_failed: List[str] = []

            for pid in failed:
                print(f"\n--- RETRY {r}: {pid} ---")
                try:
                    backoff_sleep(r)
                    row = collect_one_product(driver, pid)
                    append_row(OUT_CSV, row)
                    print(f"[RETRY OK] saved: {pid} | trades_used={row.get('n_trades_used')}")
                    human_sleep()
                except Exception as e:
                    msg = f"{type(e).__name__}: {e}"
                    print(f"[RETRY FAIL] {pid} -> {msg}")
                    next_failed.append(pid)
                    append_row(OUT_CSV, {"product_id": pid, "error": f"RETRY{r} {msg}"})
                    human_sleep(1.5)

            failed = next_failed

        if failed:
            print("\n[WARN] 끝까지 실패한 product_id:")
            print(failed)

        print(f"\n[DONE] output: {OUT_CSV}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
