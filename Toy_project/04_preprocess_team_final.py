# 04_preprocess_team_update.py
import pandas as pd
import numpy as np
import re
import warnings
import os

# ════════════════════════════════════════════════
# 파일 경로 설정 — 여기서만 수정하면 됩니다
# ════════════════════════════════════════════════
INPUT_PRODUCT_CSV = r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\preprocess_dataset\KREAM_product_21-40.csv"
INPUT_TRADE_CSV   = r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\03_google_trade_21-40.csv"
OUTPUT_CSV        = r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\04_preprocessed_21-40.csv"
# ════════════════════════════════════════════════

COLLECTION_DATE = pd.to_datetime("2026-02-19")
COLOR_CATS = ["white", "black", "grey", "brown", "pink", "blue", "navy", "green", "red", "orange", "yellow"]


def read_csv_safe(path: str, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig", **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949", **kwargs)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\s+", "", c).strip().lower() for c in df.columns]
    return df


def normalize_product_id(series: pd.Series) -> pd.Series:
    s = series.copy()
    s = s.astype(str).str.strip()
    s = s.replace({"nan": np.nan, "None": np.nan, "": np.nan})
    s = s.str.replace(r"\.0$", "", regex=True)
    return s


COLOR_SYNONYMS = {
    "white":  ["white", "오프화이트", "offwhite", "off-white", "ivory", "아이보리", "cream", "크림", "snow", "eggshell"],
    "black":  ["black", "블랙", "jetblack", "jet-black"],
    "grey":   ["grey", "gray", "그레이", "회색", "charcoal", "차콜", "slate"],
    "brown":  ["brown", "브라운", "tan", "베이지", "beige", "camel", "카멜", "mocha", "모카", "chocolate"],
    "pink":   ["pink", "핑크", "rose", "로즈", "fuchsia", "푸시아", "magenta", "마젠타", "coral", "코랄"],
    "blue":   ["blue", "블루", "sky", "스카이", "lightblue", "light-blue", "aqua", "아쿠아", "cyan", "시안", "teal", "틸"],
    "navy":   ["navy", "네이비", "midnight", "미드나잇", "darkblue", "dark-blue"],
    "green":  ["green", "그린", "olive", "올리브", "khaki", "카키", "mint", "민트", "lime", "라임", "sage", "세이지"],
    "red":    ["red", "레드", "burgundy", "버건디", "maroon", "마룬", "wine", "와인", "crimson", "크림슨"],
    "orange": ["orange", "오렌지", "tangerine", "귤색", "corange"],
    "yellow": ["yellow", "옐로우", "노랑", "mustard", "머스타드", "gold", "골드"],
}

def normalize_color_onehot(s: str) -> dict:
    out = {f"color_{c}": 0 for c in COLOR_CATS}
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return out

    text = str(s).strip().lower()
    if text in ["nan", "none", ""]:
        return out

    text = text.replace("gray", "grey")
    text = re.sub(r"[\(\)\[\]\{\}]", " ", text)
    text = re.sub(r"[\/&\+\|,;]", " ", text)
    text = re.sub(r"[-_]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    tokens = set(text.split(" "))
    haystack = " " + text + " "

    for cat, syns in COLOR_SYNONYMS.items():
        for syn in syns:
            syn_norm = syn.lower().replace("gray", "grey").replace("-", " ").replace("_", " ").strip()
            if syn_norm in tokens or f" {syn_norm} " in haystack or syn_norm in text:
                out[f"color_{cat}"] = 1
                break

    return out


def is_golden_size(x) -> int:
    if pd.isna(x):
        return 0
    s = int(float(x))
    return 1 if (235 <= s <= 245) or (265 <= s <= 275) else 0


def clean_price_to_numeric(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("원", "", regex=False)
              .replace("nan", np.nan)
              .pipe(pd.to_numeric, errors="coerce")
    )


def preprocess_product_df(product_df: pd.DataFrame) -> pd.DataFrame:
    p = normalize_columns(product_df)
    p = p.drop(columns=["product_name", "model_number", "trade_count"], errors="ignore")

    if "product_id" not in p.columns:
        raise ValueError(f"[product] product_id 컬럼 없음. 현재 컬럼: {list(p.columns)}")

    if "release_date" in p.columns:
        p["release_date"] = pd.to_datetime(p["release_date"], errors="coerce")

    if "release_price" in p.columns:
        p["release_price"] = clean_price_to_numeric(p["release_price"])

    if "wish_count" in p.columns:
        p["wish_count"] = pd.to_numeric(p["wish_count"], errors="coerce")

    if "is_collaboration" in p.columns:
        p["is_collaboration"] = p["is_collaboration"].fillna(0).astype(int)
    else:
        warnings.warn("[product] is_collaboration 없음 → 0 채움", UserWarning)
        p["is_collaboration"] = 0

    if "color" in p.columns:
        color_df = p["color"].apply(normalize_color_onehot).apply(pd.Series)
        p = pd.concat([p, color_df], axis=1)
    else:
        for c in COLOR_CATS:
            p[f"color_{c}"] = 0

    p = p.dropna(subset=["release_date", "release_price"])
    p = p[p["release_price"] > 0]

    p["product_id"] = normalize_product_id(p["product_id"])
    p = p.dropna(subset=["product_id"])
    p = p.sort_values("product_id").drop_duplicates("product_id", keep="first")
    return p


def preprocess_trade_df(trade_df: pd.DataFrame) -> pd.DataFrame:
    t = normalize_columns(trade_df)

    for col in ["product_id", "trade_date"]:
        if col not in t.columns:
            raise ValueError(f"[trade] 필수 컬럼 '{col}' 없음. 현재 컬럼: {list(t.columns)}")

    if "current_price" not in t.columns:
        if "price" in t.columns:
            t = t.rename(columns={"price": "current_price"})
        else:
            raise ValueError(f"[trade] current_price/price 없음. 현재 컬럼: {list(t.columns)}")

    t["product_id"] = normalize_product_id(t["product_id"])
    t = t.dropna(subset=["product_id"])

    t["trade_date"] = pd.to_datetime(t["trade_date"], errors="coerce")
    t["current_price"] = clean_price_to_numeric(t["current_price"])

    if "size" in t.columns:
        t["size"] = pd.to_numeric(t["size"], errors="coerce")
    else:
        t["size"] = np.nan

    for col in ["google_trend_release", "google_trend_n_day"]:
        if col in t.columns:
            t[col] = pd.to_numeric(t[col], errors="coerce")
        else:
            warnings.warn(f"[trade] '{col}' 없음 → NaN", UserWarning)
            t[col] = np.nan

    t = t.dropna(subset=["trade_date", "current_price"])
    return t


def build_trade_level_dataset(product_df: pd.DataFrame, trade_df: pd.DataFrame) -> pd.DataFrame:
    p = preprocess_product_df(product_df)
    t = preprocess_trade_df(trade_df)

    df = t.merge(p, on="product_id", how="inner", suffixes=("", "_prod"))

    prod_dup_cols = [c for c in df.columns if c.endswith("_prod")]
    if prod_dup_cols:
        df = df.drop(columns=prod_dup_cols)

    df = df.sort_values(["product_id", "trade_date"]).reset_index(drop=True)
    df["cum_trade_count_product"] = df.groupby("product_id").cumcount() + 1

    first_trade = (
        df.groupby("product_id", as_index=False)["trade_date"]
          .min()
          .rename(columns={"trade_date": "first_trade_date"})
    )
    df = df.merge(first_trade, on="product_id", how="left")

    df["days_since_first_trade"] = (df["trade_date"] - df["first_trade_date"]).dt.days
    df["golden_size"] = df["size"].apply(is_golden_size)
    df["premium_ratio"] = (df["current_price"] - df["release_price"]) / df["release_price"]

    # wish_per_day / adjusted_wish
    # adjusted_wish = wish_count × (days_since_first_trade + 1) / (COLLECTION_DATE - first_trade_date).days
    # → wish_count가 크롤링까지 균등 증가한다는 가정 하에 해당 시점의 wish 추정값
    denom_days = (COLLECTION_DATE - df["first_trade_date"]).dt.days
    denom_days = denom_days.where(denom_days > 0, np.nan)
    df["wish_per_day"]  = df["wish_count"] / denom_days
    df["adjusted_wish"] = df["wish_per_day"] * (df["days_since_first_trade"] + 1)

    # trend_retention / adjusted_google_trend
    df["trend_retention"] = df["google_trend_n_day"] / (df["google_trend_release"] + 1)
    df["adjusted_google_trend"] = (
        df["google_trend_release"] * df["google_trend_n_day"]
    ) / (df["google_trend_release"] + df["google_trend_n_day"] + 1)

    # ── 신규 피처: 수요 강도 대리 피처 3개 ──────────────────────
    # trade_velocity    : cum_trade_count / (days + 1)  → 거래 속도
    # trend_x_trade     : adjusted_google_trend × trade_velocity  → 관심 × 거래 시너지
    # early_trade_signal: 출시 7일 이내 거래면 trade_velocity, 아니면 0  → 초기 수요 신호
    df["trade_velocity"]     = df["cum_trade_count_product"] / (df["days_since_first_trade"] + 1)
    df["trend_x_trade"]      = df["adjusted_google_trend"] * df["trade_velocity"]
    df["early_trade_signal"] = (df["days_since_first_trade"] <= 7).astype(int) * df["trade_velocity"]

    # ── 신규 피처: 가격 표준편차 ────────────────────────────────
    # price_std_product : 제품별 누적 거래가격 표준편차 (expanding)
    # → 가격이 얼마나 들쑥날쑥한지 = 변동성/희소성 신호
    df["price_std_product"] = (
        df.groupby("product_id")["current_price"]
          .expanding()
          .std()
          .reset_index(level=0, drop=True)
    )

    # ── 신규 피처: 최근 7일 거래량 증가율 ───────────────────────
    # trade_growth_7d : log(최근 7일 거래량 + 1) - log(이전 7일 거래량 + 1)
    # → 양수면 거래 가속, 음수면 거래 감속
    daily_trade = (
        df.groupby(["product_id", "trade_date"])
          .size()
          .reset_index(name="daily_trade_count")
    )
    daily_trade["recent_7d"] = (
        daily_trade.groupby("product_id")["daily_trade_count"]
                   .rolling(7, min_periods=1)
                   .sum()
                   .reset_index(level=0, drop=True)
    )
    daily_trade["prev_7d"] = (
        daily_trade.groupby("product_id")["daily_trade_count"]
                   .rolling(14, min_periods=1)
                   .sum()
                   .reset_index(level=0, drop=True)
        - daily_trade["recent_7d"]
    )
    daily_trade["trade_growth_7d"] = (
        np.log1p(daily_trade["recent_7d"]) - np.log1p(daily_trade["prev_7d"])
    )
    df = df.merge(
        daily_trade[["product_id", "trade_date", "trade_growth_7d"]],
        on=["product_id", "trade_date"],
        how="left",
    )

    # ── 신규 피처: 추가 파생 피처 ─────────────────────────────────
    # days_since_release      : 출시일로부터 거래일까지 경과 일수
    # hist_avg_premium        : 직전 거래까지의 누적 평균 프리미엄 (shift(1)로 당일 값 제외)
    # rolling_price_std       : 직전 거래 기준 7일 rolling 가격 표준편차 (shift(1)로 당일 값 제외)
    # trade_interval_density  : 직전 거래 기준 최근 3건 거래 간격 합산 (짧을수록 거래 밀집)
    #                           ※ 기존 trade_velocity(누적 거래 속도)와 이름 충돌을 피해 변경
    df = df.sort_values(["product_id", "trade_date"]).reset_index(drop=True)

    df["trade_date_dt"]   = pd.to_datetime(df["trade_date"])
    df["release_date_dt"] = pd.to_datetime(df["release_date"])
    df["days_since_release"] = (df["trade_date_dt"] - df["release_date_dt"]).dt.days

    df["hist_avg_premium"] = (
        df.groupby("product_id")["premium_ratio"]
          .transform(lambda x: x.shift(1).expanding().mean())
          .fillna(0)
    )
    df["rolling_price_std"] = (
        df.groupby("product_id")["current_price"]
          .transform(lambda x: x.shift(1).rolling(window=7, min_periods=2).std())
          .fillna(0)
    )
    df["trade_interval_density"] = (
        df.groupby("product_id")["trade_date_dt"]
          .diff().dt.days
          .fillna(0)
          .transform(lambda x: x.shift(1).rolling(window=3, min_periods=1).sum())
          .fillna(0)
    )

    # status
    status_parts = pd.Series([""] * len(df), index=df.index)
    inf_mask = df["premium_ratio"].isin([np.inf, -np.inf])
    status_parts = status_parts.where(~inf_mask, status_parts + "[premium_ratio=inf] ")

    wish_nan_mask = df["adjusted_wish"].isna()
    status_parts = status_parts.where(~wish_nan_mask, status_parts + "[adjusted_wish=NaN] ")

    for col in ["google_trend_release", "google_trend_n_day", "trend_retention", "adjusted_google_trend"]:
        mask = df[col].isna()
        status_parts = status_parts.where(~mask, status_parts + f"[{col}=NaN] ")

    neg_price_mask = df["current_price"] < 0
    status_parts = status_parts.where(~neg_price_mask, status_parts + "[current_price<0] ")

    df["status"] = status_parts.str.strip().replace("", "-")

    return df.copy()


if __name__ == "__main__":
    print(f"[전처리] 제품 CSV 읽는 중: {INPUT_PRODUCT_CSV}")
    product_df = read_csv_safe(INPUT_PRODUCT_CSV)

    print(f"[전처리] 거래 CSV 읽는 중: {INPUT_TRADE_CSV}")
    trade_df = read_csv_safe(INPUT_TRADE_CSV)

    print("[전처리] 전처리 시작...")
    result = build_trade_level_dataset(product_df, trade_df)

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    result.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"[전처리] 완료! 저장 위치: {OUTPUT_CSV}")
    print(f"         행 수: {len(result):,} | 컬럼 수: {len(result.columns)}")