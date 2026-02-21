# 04_preprocess_team_update.py
import pandas as pd
import numpy as np
import re
import warnings
import os

# ════════════════════════════════════════════════
# 파일 경로 설정 — 여기서만 수정하면 됩니다
# ════════════════════════════════════════════════
INPUT_PRODUCT_CSV = r"C:\Users\lg\min_python\KHUDA\9_ML_TP_KREAM\preprocess_dataset\KREAM_product_101-120.csv"
INPUT_TRADE_CSV   = r"C:\Users\lg\min_python\KHUDA\9_ML_TP_KREAM\outputs\03_google_trade_101-120.csv"
OUTPUT_CSV        = r"C:\Users\lg\min_python\KHUDA\9_ML_TP_KREAM\outputs\04_preprocessed_101-120.csv"
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
    """
    product_id를 merge-safe한 문자열로 통일.
    - 21.0 같은 float 표현 제거
    - NaN 방어
    """
    s = series.copy()
    s = s.astype(str).str.strip()
    s = s.replace({"nan": np.nan, "None": np.nan, "": np.nan})
    s = s.str.replace(r"\.0$", "", regex=True)
    return s


import re

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
    """
    색상 문자열 표기 흔들림을 강하게 정규화해서 one-hot 생성.
    - 대소문자 통일, 특수문자 제거, 공백/슬래시 분리 대응
    - off-white/ivory/cream 등도 white로 매핑
    - navy/blue 분리 유지
    """
    out = {f"color_{c}": 0 for c in COLOR_CATS}
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return out

    text = str(s).strip().lower()
    if text in ["nan", "none", ""]:
        return out

    # 구분자 통일 (/, &, +, ',', ';', '|', '-' 등)
    text = text.replace("gray", "grey")
    text = re.sub(r"[\(\)\[\]\{\}]", " ", text)
    text = re.sub(r"[\/&\+\|,;]", " ", text)     # 주요 구분자 -> 공백
    text = re.sub(r"[-_]", " ", text)            # 하이픈/언더스코어도 분리
    text = re.sub(r"\s+", " ", text).strip()

    # 단어 단위/부분 문자열 모두 잡기 위해 토큰 + 전체문자열 둘 다 사용
    tokens = set(text.split(" "))
    haystack = " " + text + " "

    # 각 색상별 동의어 중 하나라도 매칭되면 1
    for cat, syns in COLOR_SYNONYMS.items():
        for syn in syns:
            syn_norm = syn.lower().replace("gray", "grey").replace("-", " ").replace("_", " ").strip()
            # 토큰 매칭 + 부분문자열 매칭(예: 'off white' / 'dark blue')
            if syn_norm in tokens or f" {syn_norm} " in haystack or syn_norm in text:
                out[f"color_{cat}"] = 1
                break

    # navy가 잡혔는데 blue도 같이 잡히는 경우가 많아서, 둘 다 1 허용(복합색이면 자연스러움)
    # 만약 "navy면 blue는 0" 같은 정책을 원하면 아래 줄을 활성화:
    # if out["color_navy"] == 1: out["color_blue"] = 0

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

    # 불필요 컬럼 제거
    p = p.drop(columns=["product_name", "model_number", "trade_count"], errors="ignore")

    if "product_id" not in p.columns:
        raise ValueError(f"[product] product_id 컬럼 없음. 현재 컬럼: {list(p.columns)}")

    # 타입
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

    # color one-hot
    if "color" in p.columns:
        color_df = p["color"].apply(normalize_color_onehot).apply(pd.Series)
        p = pd.concat([p, color_df], axis=1)
    else:
        for c in COLOR_CATS:
            p[f"color_{c}"] = 0

    # 필수 결측 제거
    p = p.dropna(subset=["release_date", "release_price"])
    p = p[p["release_price"] > 0]

    # ✅ product_id 문자열 통일 (merge 에러 방지)
    p["product_id"] = normalize_product_id(p["product_id"])
    p = p.dropna(subset=["product_id"])

    # 중복 제거
    p = p.sort_values("product_id").drop_duplicates("product_id", keep="first")
    return p


def preprocess_trade_df(trade_df: pd.DataFrame) -> pd.DataFrame:
    t = normalize_columns(trade_df)

    # 필수 컬럼
    for col in ["product_id", "trade_date"]:
        if col not in t.columns:
            raise ValueError(f"[trade] 필수 컬럼 '{col}' 없음. 현재 컬럼: {list(t.columns)}")

    # 03 결과물에 current_price가 있어야 함 (없으면 03부터 수정 필요)
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

    # google trend (pre 없음)
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

    # merge
    df = t.merge(p, on="product_id", how="inner", suffixes=("", "_prod"))

    prod_dup_cols = [c for c in df.columns if c.endswith("_prod")]
    if prod_dup_cols:
        df = df.drop(columns=prod_dup_cols)

    # ✅ 누적 거래량 (제품별, 거래일 기준 누적)
    df = df.sort_values(["product_id", "trade_date"]).reset_index(drop=True)
    df["cum_trade_count_product"] = df.groupby("product_id").cumcount() + 1

    # first_trade_date
    first_trade = (
        df.groupby("product_id", as_index=False)["trade_date"]
          .min()
          .rename(columns={"trade_date": "first_trade_date"})
    )
    df = df.merge(first_trade, on="product_id", how="left")

    # days_since_first_trade
    df["days_since_first_trade"] = (df["trade_date"] - df["first_trade_date"]).dt.days

    # golden_size
    df["golden_size"] = df["size"].apply(is_golden_size)

    # premium_ratio
    df["premium_ratio"] = (df["current_price"] - df["release_price"]) / df["release_price"]

    # wish_per_day
    denom_days = (COLLECTION_DATE - df["first_trade_date"]).dt.days
    denom_days = denom_days.where(denom_days > 0, np.nan)
    df["wish_per_day"] = df["wish_count"] / denom_days

    # adjusted_wish
    df["adjusted_wish"] = df["wish_per_day"] * (df["days_since_first_trade"] + 1)

    # trend_retention + adjusted_google_trend (pre 없음)
    df["trend_retention"] = df["google_trend_n_day"] / (df["google_trend_release"] + 1)
    df["adjusted_google_trend"] = (
        df["google_trend_release"] * df["google_trend_n_day"]
    ) / (df["google_trend_release"] + df["google_trend_n_day"] + 1)

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

    color_cols = [f"color_{c}" for c in COLOR_CATS]
    final_cols = (
        ["product_id", "release_date", "release_price", "is_collaboration"]
        + color_cols
        + ["size", "golden_size"]
        + ["first_trade_date", "days_since_first_trade", "trade_date", "current_price", "premium_ratio"]
        + ["wish_count", "wish_per_day", "adjusted_wish"]
        + ["google_trend_release", "google_trend_n_day", "trend_retention", "adjusted_google_trend"]
        + ["cum_trade_count_product"]
        + ["status"]
    )

    for c in final_cols:
        if c not in df.columns:
            warnings.warn(f"[전처리] '{c}' 없음 → NaN", UserWarning)
            df[c] = np.nan

    return df[final_cols].copy()


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
