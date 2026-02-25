# 01_data_collection.py
# New Balance 50 products dynamic crawling with stability:
# - persistent Chrome profile (login session reuse)
# - auto click/scroll (no manual input during crawling)
# - checkpoint CSV append per product
# - retry queue
# - captcha/block suspected -> restart GUI and let user solve manually

import os, re, csv, time, random, traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# =========================
# CONFIG
# =========================
BRAND_URL = "https://kream.co.kr/brands/New%20Balance?tab=44"
OUT_CSV = "newbalance_50.csv"
TARGET_N = 50

# Persistent Chrome profile directory (VERY IMPORTANT for login reuse)
# Change this path to your machine path if you want.
PROFILE_DIR = os.path.abspath("./chrome_profile_kream")

# Crawl behavior
HEADLESS_DEFAULT = False  # recommend False for stability (captcha/login)
PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 10
SLEEP_MIN = 0.9
SLEEP_MAX = 1.9

# Golden sizes (team result)
GOLDEN_UNISEX = {"265", "270"}   # you can include 275 if needed
GOLDEN_WOMEN = {"235", "240", "245"}

# Parsing selectors (from your notebook logic)
ROW_CLASS = "body_list"
COL_CLASS = "list_txt"


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


def try_click_any(driver: webdriver.Chrome, candidates: List[Tuple[By, str]], timeout_each=3) -> bool:
    for by, sel in candidates:
        if wait_click(driver, by, sel, timeout=timeout_each):
            return True
    return False


def human_sleep():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


# =========================
# CAPTCHA/BLOCK DETECTION (heuristic)
# =========================
def is_captcha_or_block(driver: webdriver.Chrome) -> bool:
    try:
        url = (driver.current_url or "").lower()
        html = (driver.page_source or "").lower()
    except Exception:
        return True

    keywords = [
        "captcha", "recaptcha", "verify you are human",
        "access denied", "blocked", "robot",
        "로봇", "자동", "비정상", "차단", "보안", "인증"
    ]
    if any(k in url for k in keywords):
        return True
    if any(k in html for k in keywords):
        return True
    return False


def restart_gui_for_manual_fix(driver: webdriver.Chrome, profile_dir: str) -> webdriver.Chrome:
    print("\n[BLOCK] 캡챠/차단 또는 로그인 이슈가 의심됩니다.")
    print("        GUI 모드로 브라우저를 다시 열고, 직접 해결한 뒤 엔터를 누르세요.")
    try:
        driver.quit()
    except Exception:
        pass

    driver = make_driver(headless=False, profile_dir=profile_dir)
    driver.get("https://kream.co.kr")
    input("👉 (수동) 로그인/캡챠 해결 완료 후 엔터...")
    return driver


# =========================
# CHECKPOINT CSV (append)
# =========================
FIELD_ORDER = [
    "product_id", "한글명", "관심수",
    "모델번호", "발매일", "발매가", "색상",
    "is_womens",
    "Week1_Avg", "Week2_Avg", "Week3_Avg", "Week4_Avg",
    "error"
]


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
        # ensure all keys exist
        safe = {k: row.get(k, None) for k in FIELD_ORDER}
        w.writerow(safe)


# =========================
# BRAND PRODUCT IDS (dynamic scroll)
# =========================
def extract_product_ids_from_html(html: str) -> Set[str]:
    # /products/123456 pattern
    return set(re.findall(r"/products/(\d+)", html or ""))


def scroll_window(driver: webdriver.Chrome, steps=3, pause=0.9):
    for _ in range(steps):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)


def collect_brand_product_ids(driver: webdriver.Chrome, brand_url: str, target_n=50, max_scroll_rounds=60) -> List[str]:
    driver.get(brand_url)
    time.sleep(2.0)

    ids: Set[str] = set()
    prev = 0
    stagnation = 0

    for _ in range(max_scroll_rounds):
        ids |= extract_product_ids_from_html(driver.page_source)

        if len(ids) == prev:
            stagnation += 1
        else:
            stagnation = 0
            prev = len(ids)

        if len(ids) >= target_n:
            break

        scroll_window(driver, steps=2, pause=0.9)

        if stagnation >= 8:
            break

    return list(ids)[:target_n]


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
    time.sleep(1.3)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    info = {"한글명": "Unknown", "관심수": 0}

    # Wish count (matches your notebook selector style)
    target_selector = f'[data-sdui-id="product_wish_count/{product_id}"]'
    wish_el = soup.select_one(target_selector)
    if not wish_el:
        wish_el = soup.select_one('[data-sdui-id*="product_wish_count"]')

    if wish_el:
        info["관심수"] = parse_wish_count_text(wish_el.get_text(strip=True))

    # Korean name parsing (style-based like your notebook)
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
    # UI changes: keep broad candidates
    candidates = [
        (By.XPATH, "//*[contains(text(),'혜택 더보기')]/ancestor::button[1]"),
        (By.XPATH, "//*[contains(text(),'더보기')]/ancestor::button[1]"),
        (By.XPATH, "//*[contains(text(),'상세')]/ancestor::button[1]"),
        # icon button fallback
        (By.XPATH, "//button//*[name()='svg']/ancestor::button[1]"),
    ]
    try_click_any(driver, candidates, timeout_each=2)
    time.sleep(0.4)


def get_kream_details_auto(driver: webdriver.Chrome, product_id: str) -> Dict:
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
# TRANSACTIONS (open tab + sort oldest + auto scroll + 30 days)
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


def open_transactions_tab(driver: webdriver.Chrome):
    candidates = [
        (By.XPATH, "//*[contains(text(),'체결') and contains(text(),'내역')]/ancestor::*[self::a or self::button][1]"),
        (By.XPATH, "//a[contains(.,'체결')]"),
        (By.XPATH, "//button[contains(.,'체결')]"),
    ]
    try_click_any(driver, candidates, timeout_each=3)
    time.sleep(0.6)


def set_sort_oldest(driver: webdriver.Chrome):
    # Sorting UI changes often; best-effort
    sort_btn_candidates = [
        (By.XPATH, "//*[contains(text(),'정렬')]/ancestor::*[self::a or self::button][1]"),
        (By.XPATH, "//*[contains(@class,'sort')]/ancestor::*[self::a or self::button][1]"),
    ]
    try_click_any(driver, sort_btn_candidates, timeout_each=2)
    time.sleep(0.2)

    oldest_candidates = [
        (By.XPATH, "//*[contains(text(),'과거')]/ancestor::*[self::a or self::button][1]"),
        (By.XPATH, "//*[contains(text(),'오래')]/ancestor::*[self::a or self::button][1]"),
    ]
    try_click_any(driver, oldest_candidates, timeout_each=2)
    time.sleep(0.4)


def auto_scroll_until_one_month(driver: webdriver.Chrome,
                                release_dt: Optional[datetime.date],
                                today_date: datetime.date,
                                max_rounds=60):
    last_count = 0
    stagnant = 0

    for _ in range(max_rounds):
        soup = BeautifulSoup(driver.page_source, "html.parser")
        rows = soup.find_all("div", class_=ROW_CLASS)

        if len(rows) <= last_count:
            stagnant += 1
        else:
            stagnant = 0
            last_count = len(rows)

        # If release date known, stop when we see >30 days since release
        if release_dt and rows:
            for row in rows[-20:]:
                cols = row.find_all("div", class_=COL_CLASS)
                if len(cols) >= 3:
                    d = parse_kream_date(today_date, cols[2].get_text(strip=True))
                    if d and (d - release_dt).days > 30:
                        return

        if stagnant >= 6:
            return

        # Scroll down (window)
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
        time.sleep(0.85)


def get_kream_transactions_1month_auto(driver: webdriver.Chrome,
                                      release_dt_str: Optional[str],
                                      is_womens: bool) -> Dict:
    today_date = datetime.now().date()

    release_dt = None
    if release_dt_str:
        try:
            release_dt = pd.to_datetime("20" + release_dt_str, format="%Y/%m/%d").date()
        except Exception:
            release_dt = None

    open_transactions_tab(driver)
    set_sort_oldest(driver)

    # scroll to load enough rows
    auto_scroll_until_one_month(driver, release_dt, today_date, max_rounds=70)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.find_all("div", class_=ROW_CLASS)

    golden = GOLDEN_WOMEN if is_womens else GOLDEN_UNISEX
    tx = []

    for row in rows:
        cols = row.find_all("div", class_=COL_CLASS)
        if len(cols) < 3:
            continue

        size_raw = cols[0].get_text(strip=True)
        m = re.search(r"\d{3}", size_raw)
        if not m:
            continue
        size_num = m.group()
        if size_num not in golden:
            continue

        price_str = cols[1].get_text(strip=True)
        if "원" not in price_str:
            continue
        price_num = int(re.sub(r"[^0-9]", "", price_str))

        date_str = cols[2].get_text(strip=True)
        trade_date = parse_kream_date(today_date, date_str)
        if not trade_date:
            continue

        # if release date unknown, set baseline as first seen trade date (oldest sort assumed)
        if release_dt is None:
            release_dt = trade_date

        days_since = (trade_date - release_dt).days
        if days_since < 0:
            continue
        if days_since > 30:
            break

        tx.append({"price": price_num, "days_since": days_since})

    df = pd.DataFrame(tx)
    if df.empty:
        return {"Week1_Avg": None, "Week2_Avg": None, "Week3_Avg": None, "Week4_Avg": None}

    df["week_idx"] = df["days_since"] // 7
    wk = df.groupby("week_idx")["price"].mean()

    def _get(i):
        return int(wk.get(i)) if i in wk else None

    return {"Week1_Avg": _get(0), "Week2_Avg": _get(1), "Week3_Avg": _get(2), "Week4_Avg": _get(3)}


# =========================
# SINGLE PRODUCT COLLECT
# =========================
def collect_one_product(driver: webdriver.Chrome, product_id: str) -> Dict:
    # Basic
    basic = get_kream_basic_info(driver, product_id)
    name_ko = basic.get("한글명", "Unknown")
    wish = basic.get("관심수", 0)

    # Women 판단: 팀 규칙 유지
    is_womens = "(W)" in (name_ko or "")

    # Details
    details = get_kream_details_auto(driver, product_id)

    # Transactions (must already be on product page; details uses same page_source)
    tx = get_kream_transactions_1month_auto(driver, details.get("발매일"), is_womens)

    row = {
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
    return row


# =========================
# MAIN RUN (50 products + retry + checkpoint)
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
        # First-time login prompt (safe)
        driver.get("https://kream.co.kr")
        print("\n[STEP] 처음 실행이면 로그인 해주세요. (프로필로 세션 유지됩니다)")
        input("👉 로그인 완료 후 엔터...")

        # collect product IDs
        product_ids = collect_brand_product_ids(driver, brand_url, target_n=target_n)
        print(f"[OK] 뉴발란스 product_id 수집: {len(product_ids)}개")

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

                # save error row for trace (optional)
                append_row(out_csv, {
                    "product_id": str(pid),
                    "error": msg
                })

                # captcha/block handling
                try:
                    if is_captcha_or_block(driver):
                        driver = restart_gui_for_manual_fix(driver, profile_dir)
                except Exception:
                    # if anything odd, restart GUI anyway
                    driver = restart_gui_for_manual_fix(driver, profile_dir)

        # retry rounds (failed ids only)
        for r in range(1, retry_rounds + 1):
            if not failed:
                break

            # remove already done
            done_ids = load_done_ids(out_csv)
            failed = [pid for pid in failed if pid not in done_ids]

            if not failed:
                break

            print(f"\n[RETRY ROUND {r}] 재시도 대상: {len(failed)}개")
            next_failed = []

            for pid in failed:
                try:
                    backoff_sleep(r)
                    row = collect_one_product(driver, pid)
                    append_row(out_csv, row)
                    human_sleep()
                except Exception as e:
                    msg = f"{type(e).__name__}: {e}"
                    print(f"  [RETRY FAIL] {pid} -> {msg}")
                    next_failed.append(pid)
                    append_row(out_csv, {"product_id": pid, "error": f"RETRY{r} {msg}"})

                    try:
                        if is_captcha_or_block(driver):
                            driver = restart_gui_for_manual_fix(driver, profile_dir)
                    except Exception:
                        driver = restart_gui_for_manual_fix(driver, profile_dir)

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
    # 처음엔 5개로 테스트 권장 → 문제 없으면 50으로
    # run_newbalance_collection(target_n=5, headless=False, out_csv="newbalance_test.csv")
    run_newbalance_collection(target_n=50, headless=False, out_csv="newbalance_50.csv")
