# 01_data_collection_list.py
# Stable KREAM crawler for fixed product_id list (New Balance)
# - manual id list (txt/csv) for reproducibility
# - login session reuse via chrome profile
# - close search overlay (the "popular search" page) safety
# - checkpoint append CSV per product
# - retr

# Output:
#   newbalance_50_list.csv

import os, re, csv, time, random, traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from webdriver_manager.chrome import ChromeDriverManager


# =========================
# CONFIG
# =========================
ID_TXT = "product_id_popularity.txt"
ID_CSV = "product_id_popularity.csv"

OUT_CSV = "newbalance_50_list.csv"
PROFILE_DIR = os.path.abspath("./chrome_profile_kream")  # login session reuse

HEADLESS = False  # recommend False for captcha/login stability
PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 10
SLEEP_MIN = 0.9
SLEEP_MAX = 1.8

# Golden sizes (your team's results)
GOLDEN_UNISEX = {"265", "270"}
GOLDEN_WOMEN = {"235", "240", "245"}

# transaction row selectors (from your notebook)
ROW_CLASS = "body_list"
COL_CLASS = "list_txt"

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

    # persistent session
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--profile-directory=Default")

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def human_sleep(mult: float = 1.0):
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX) * mult)


def wwait(driver, timeout=WAIT_TIMEOUT):
    return WebDriverWait(driver, timeout)


def try_click_any(driver, candidates, timeout_each=2) -> bool:
    for by, sel in candidates:
        try:
            el = wwait(driver, timeout_each).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            continue
    return False


# =========================
# OVERLAY / WRONG PAGE GUARDS
# =========================
def close_search_overlay(driver):
    """
    The screenshot page (popular search keywords) is usually an overlay with a close X.
    Try ESC, then try clicking close buttons widely.
    """
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).send_keys(Keys.ESCAPE).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.2)
    except Exception:
        pass

    close_candidates = [
        (By.XPATH, "//*[@aria-label='닫기']/ancestor::button[1]"),
        (By.XPATH, "//*[@aria-label='close']/ancestor::button[1]"),
        (By.XPATH, "//button[contains(@class,'close') or contains(@class,'btn_close')]"),
        (By.XPATH, "//*[text()='닫기']/ancestor::button[1]"),
        # overlay top-right X svg button (broad)
        (By.XPATH, "//div[contains(@class,'layer') or contains(@class,'overlay') or contains(@class,'modal')]"
                   "//button//*[name()='svg']/ancestor::button[1]"),
    ]
    try_click_any(driver, close_candidates, timeout_each=1)
    time.sleep(0.2)


def ensure_on_product_page(driver, product_id: str):
    """
    If we got navigated to search/trend overlay or other page, force back to product page.
    """
    url = (driver.current_url or "")
    if f"/products/{product_id}" not in url:
        driver.get(f"https://kream.co.kr/products/{product_id}")
        time.sleep(1.0)
    close_search_overlay(driver)


def is_captcha_or_block(driver) -> bool:
    """
    Heuristic only. We do NOT bypass captchas.
    If suspected, we ask user to solve manually.
    """
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
    return any(k in url for k in keywords) or any(k in html for k in keywords)


def manual_fix_login_or_captcha(driver, profile_dir: str) -> webdriver.Chrome:
    """
    Restart GUI and let user solve captcha/login.
    """
    print("\n[BLOCK] 로그인/캡챠/차단 의심. 브라우저를 GUI로 재시작합니다.")
    try:
        driver.quit()
    except Exception:
        pass

    driver = make_driver(headless=False, profile_dir=profile_dir)
    driver.get("https://kream.co.kr")
    input("👉 (수동) 로그인/캡챠 해결 후 엔터를 누르세요...")
    return driver


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
# LOAD IDS (txt/csv)
# =========================
def load_product_ids() -> List[str]:
    if os.path.exists(ID_TXT):
        ids = []
        with open(ID_TXT, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
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
# PARSERS (from your notebook style)
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

    target_selector = f'[data-sdui-id="product_wish_count/{product_id}"]'
    wish_el = soup.select_one(target_selector) or soup.select_one('[data-sdui-id*="product_wish_count"]')
    if wish_el:
        info["관심수"] = parse_wish_count_text(wish_el.get_text(strip=True))

    # Korean name (style pattern used in your notebook)
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
        (By.XPATH, "//button//*[name()='svg']/ancestor::button[1]"),
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


def open_transactions_tab(driver):
    close_search_overlay(driver)
    # try broad candidates (UI changes)
    candidates = [
        (By.XPATH, "//*[contains(text(),'체결') and contains(text(),'내역')]/ancestor::*[self::a or self::button][1]"),
        (By.XPATH, "//*[contains(text(),'거래') and contains(text(),'내역')]/ancestor::*[self::a or self::button][1]"),
        (By.XPATH, "//a[contains(.,'체결') or contains(.,'거래')]"),
        (By.XPATH, "//button[contains(.,'체결') or contains(.,'거래')]"),
    ]
    try_click_any(driver, candidates, timeout_each=3)
    time.sleep(0.5)


def set_sort_oldest(driver):
    close_search_overlay(driver)
    sort_btn_candidates = [
        (By.XPATH, "//*[contains(text(),'정렬')]/ancestor::*[self::a or self::button][1]"),
        (By.XPATH, "//*[contains(@class,'sort')]/ancestor::*[self::a or self::button][1]"),
    ]
    try_click_any(driver, sort_btn_candidates, timeout_each=2)
    time.sleep(0.2)

    oldest_candidates = [
        (By.XPATH, "//*[contains(text(),'과거')]/ancestor::*[self::a or self::button or self::li][1]"),
        (By.XPATH, "//*[contains(text(),'오래')]/ancestor::*[self::a or self::button or self::li][1]"),
        (By.XPATH, "//*[contains(text(),'오래된')]/ancestor::*[self::a or self::button or self::li][1]"),
    ]
    try_click_any(driver, oldest_candidates, timeout_each=2)
    time.sleep(0.4)


def auto_scroll_until_one_month(driver,
                                release_dt: Optional[datetime.date],
                                today_date: datetime.date,
                                max_rounds=70):
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

        # stop early if we already saw a date older than release+30
        if release_dt and rows:
            for row in rows[-20:]:
                cols = row.find_all("div", class_=COL_CLASS)
                if len(cols) >= 3:
                    d = parse_kream_date(today_date, cols[2].get_text(strip=True))
                    if d and (d - release_dt).days > 30:
                        return

        if stagnant >= 6:
            return

        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
        time.sleep(0.85)


def get_kream_transactions_1month_auto(driver,
                                      product_id: str,
                                      release_dt_str: Optional[str],
                                      is_womens: bool) -> Dict:
    ensure_on_product_page(driver, product_id)

    today_date = datetime.now().date()
    release_dt = None
    if release_dt_str:
        try:
            release_dt = pd.to_datetime("20" + release_dt_str, format="%Y/%m/%d").date()
        except Exception:
            release_dt = None

    open_transactions_tab(driver)
    set_sort_oldest(driver)
    auto_scroll_until_one_month(driver, release_dt, today_date, max_rounds=80)

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

        # if release unknown, use first observed trade date as baseline (assumes oldest sort worked)
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
def collect_one_product(driver, product_id: str) -> Dict:
    # go to product page explicitly
    driver.get(f"https://kream.co.kr/products/{product_id}")
    time.sleep(1.0)
    close_search_overlay(driver)

    basic = get_kream_basic_info(driver, product_id)
    name_ko = basic.get("한글명", "Unknown")
    wish = basic.get("관심수", 0)

    is_womens = "(W)" in (name_ko or "")

    details = get_kream_details_auto(driver, product_id)
    tx = get_kream_transactions_1month_auto(driver, product_id, details.get("발매일"), is_womens)

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
# MAIN RUN: checkpoint + retry
# =========================
def backoff_sleep(attempt_round: int):
    t = min(2 ** attempt_round, 20) + random.uniform(0.3, 1.2)
    time.sleep(t)


def main():
    product_ids = load_product_ids()
    print(f"[INFO] ID 파일에서 불러온 product_id: {len(product_ids)}개")

    done_ids = load_done_ids(OUT_CSV)
    todo = [pid for pid in product_ids if pid not in done_ids]
    print(f"[INFO] 이미 수집된 id: {len(done_ids)}개, 이번 실행 대상: {len(todo)}개")

    driver = make_driver(headless=HEADLESS, profile_dir=PROFILE_DIR)

    try:
        # first-time login prompt (session reused via profile)
        driver.get("https://kream.co.kr")
        print("\n[STEP] 처음 실행이면 로그인 해주세요. (PROFILE_DIR로 세션 유지)")
        input("👉 로그인 완료 후 엔터...")

        failed: List[str] = []

        # pass 1
        for i, pid in enumerate(todo, 1):
            print(f"\n[{i}/{len(todo)}] product_id={pid}")

            try:
                row = collect_one_product(driver, pid)
                append_row(OUT_CSV, row)
                human_sleep()

            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                print(f"  [FAIL] {pid} -> {msg}")
                failed.append(pid)
                append_row(OUT_CSV, {"product_id": pid, "error": msg})

                # suspected captcha/block -> manual solve
                try:
                    if is_captcha_or_block(driver):
                        driver = manual_fix_login_or_captcha(driver, PROFILE_DIR)
                except Exception:
                    driver = manual_fix_login_or_captcha(driver, PROFILE_DIR)

        # retry queue
        retry_rounds = 2
        for r in range(1, retry_rounds + 1):
            if not failed:
                break

            done_ids = load_done_ids(OUT_CSV)
            failed = [pid for pid in failed if pid not in done_ids]
            if not failed:
                break

            print(f"\n[RETRY ROUND {r}] 재시도 대상: {len(failed)}개")
            next_failed: List[str] = []

            for pid in failed:
                print(f"  [RETRY {r}] {pid}")
                try:
                    backoff_sleep(r)
                    row = collect_one_product(driver, pid)
                    append_row(OUT_CSV, row)
                    human_sleep()
                except Exception as e:
                    msg = f"{type(e).__name__}: {e}"
                    print(f"    [RETRY FAIL] {pid} -> {msg}")
                    next_failed.append(pid)
                    append_row(OUT_CSV, {"product_id": pid, "error": f"RETRY{r} {msg}"})

                    try:
                        if is_captcha_or_block(driver):
                            driver = manual_fix_login_or_captcha(driver, PROFILE_DIR)
                    except Exception:
                        driver = manual_fix_login_or_captcha(driver, PROFILE_DIR)

            failed = next_failed

        if failed:
            print("\n[WARN] 끝까지 실패한 product_id:")
            print(failed)

        print(f"\n[DONE] 저장 완료: {OUT_CSV}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
