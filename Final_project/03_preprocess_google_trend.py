# 03_preprocess_google_trend.py
# 월별 Google Trends CSV(Nike) → 거래일/발매일에 매핑 + trade 가격 컬럼 포함해서 출력
#
# ✅ pytrends 사용 안 함
# ✅ 입력:
#   - KREAM_product_*.csv  (product_id, release_date)
#   - KREAM_trade_*.csv    (product_id, trade_date, price/current_price, size ... )
#   - Google Trends CSV    (Time, Nike)  # 월별 값 (YYYY-MM-01)
#
# ✅ 출력:
#   - 03_google_trade_*.csv
#       product_id, trade_date, current_price(정규화), size(있으면),
#       google_trend_release, google_trend_n_day

import os
from typing import Optional
import pandas as pd
import numpy as np

# =========================================================
# ✅ 파일 경로 설정 — 여기만 수정하면 됩니다
# =========================================================
INPUT_PRODUCTS_CSV = r"C:\Users\lg\min_python\KHUDA\9_ML_TP_KREAM\preprocess_dataset\KREAM_product_101-120.csv"
INPUT_TRADES_CSV   = r"C:\Users\lg\min_python\KHUDA\9_ML_TP_KREAM\preprocess_dataset\KREAM_trade_101-120.csv"

INPUT_TRENDS_CSV   = r"C:\Users\lg\min_python\KHUDA\9_ML_TP_KREAM\preprocess_dataset\time_series_KR_20190725-0000_20260220-2213.csv"

OUTPUT_CSV         = r"C:\Users\lg\min_python\KHUDA\9_ML_TP_KREAM\outputs\03_google_trade_101-120.csv"

COLLECTION_DATE    = pd.to_datetime("2026-02-19")
TREND_COL_NAME     = "Nike"
TIME_COL_NAME      = "Time"
DAILY_MAPPING_MODE = "interpolate"   # "interpolate" or "step"
# =========================================================


def read_csv_safe(filepath: str, **kwargs) -> pd.DataFrame:
    """utf-8-sig 우선 → 실패 시 cp949"""
    try:
        return pd.read_csv(filepath, encoding="utf-8-sig", **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(filepath, encoding="cp949", **kwargs)


def load_daily_trend_series(
    trends_csv: str,
    time_col: str = "Time",
    value_col: str = "Nike",
    end_date: Optional[pd.Timestamp] = None,
    mode: str = "interpolate",
) -> pd.Series:
    df = read_csv_safe(trends_csv)

    # 컬럼명 공백 제거만 (원본 대소문자 대응)
    df.columns = [c.strip() for c in df.columns]

    if time_col not in df.columns:
        low = {c.lower(): c for c in df.columns}
        if time_col.lower() in low:
            time_col = low[time_col.lower()]
        else:
            raise ValueError(f"[트렌드 CSV] 시간 컬럼 '{time_col}' 없음. 현재 컬럼: {list(df.columns)}")

    if value_col not in df.columns:
        low = {c.lower(): c for c in df.columns}
        if value_col.lower() in low:
            value_col = low[value_col.lower()]
        else:
            raise ValueError(f"[트렌드 CSV] 값 컬럼 '{value_col}' 없음. 현재 컬럼: {list(df.columns)}")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col]).sort_values(time_col)

    s_m = df.set_index(time_col)[value_col].astype(float).sort_index()

    if mode == "step":
        s_d = s_m.resample("D").ffill()
    else:
        s_d = s_m.resample("D").interpolate("time").ffill()

    if end_date is not None:
        end_date = pd.to_datetime(end_date).normalize()
        if end_date > s_d.index.max():
            extra_idx = pd.date_range(s_d.index.max() + pd.Timedelta(days=1), end_date, freq="D")
            if len(extra_idx) > 0:
                tail = pd.Series([s_d.iloc[-1]] * len(extra_idx), index=extra_idx)
                s_d = pd.concat([s_d, tail])

    return s_d


def trend_value_on_date(daily_series: pd.Series, d: pd.Timestamp) -> Optional[int]:
    if daily_series is None or d is None or pd.isna(d):
        return None
    d = pd.to_datetime(d, errors="coerce")
    if pd.isna(d):
        return None
    d = d.normalize()

    if d <= daily_series.index.min():
        return int(daily_series.iloc[0])
    if d >= daily_series.index.max():
        return int(daily_series.iloc[-1])

    try:
        return int(daily_series.loc[d])
    except Exception:
        # 혹시라도 누락이면 asof
        sub = daily_series.loc[:d]
        if len(sub) == 0:
            return int(daily_series.iloc[0])
        return int(sub.iloc[-1])


def clean_price_to_numeric(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
              .str.replace(",", "", regex=False)
              .str.replace("원", "", regex=False)
              .replace("nan", np.nan)
              .pipe(pd.to_numeric, errors="coerce")
    )


if __name__ == "__main__":
    print("[03] Google Trend 매핑 + 가격 포함 출력 시작")

    # 1) 입력 로드
    print("[INFO] products/trades 읽는 중...")
    products = read_csv_safe(INPUT_PRODUCTS_CSV, dtype={"product_id": str})
    trades   = read_csv_safe(INPUT_TRADES_CSV,   dtype={"product_id": str})

    # 컬럼명 소문자 통일 + 공백 제거
    products.columns = [c.strip().lower() for c in products.columns]
    trades.columns   = [c.strip().lower() for c in trades.columns]

    # 필수 체크
    for col in ["product_id", "release_date"]:
        if col not in products.columns:
            raise ValueError(f"[products] 필수 컬럼 누락: '{col}' (현재: {list(products.columns)})")
    for col in ["product_id", "trade_date"]:
        if col not in trades.columns:
            raise ValueError(f"[trades] 필수 컬럼 누락: '{col}' (현재: {list(trades.columns)})")

    # ✅ price/current_price 유연 대응
    if "current_price" not in trades.columns:
        if "price" in trades.columns:
            trades = trades.rename(columns={"price": "current_price"})
        else:
            raise ValueError(f"[trades] price/current_price 컬럼이 없습니다. 현재 컬럼: {list(trades.columns)}")

    # dtype
    products["release_date"] = pd.to_datetime(products["release_date"], errors="coerce")
    trades["trade_date"]     = pd.to_datetime(trades["trade_date"], errors="coerce")
    trades["current_price"]  = clean_price_to_numeric(trades["current_price"])

    # size는 있으면 숫자로
    if "size" in trades.columns:
        trades["size"] = pd.to_numeric(trades["size"], errors="coerce")

    # 필수 결측 제거
    trades = trades.dropna(subset=["trade_date", "current_price"]).copy()

    print(f"[INFO] product rows: {len(products)} / trade rows(valid): {len(trades)}")

    # 2) 트렌드 CSV → 일별 시리즈로 변환
    print("[INFO] Google Trends CSV 로드 중...")
    daily_trend = load_daily_trend_series(
        INPUT_TRENDS_CSV,
        time_col=TIME_COL_NAME,
        value_col=TREND_COL_NAME,
        end_date=COLLECTION_DATE,
        mode=DAILY_MAPPING_MODE
    )
    print(f"[INFO] daily trend range: {daily_trend.index.min().date()} ~ {daily_trend.index.max().date()} (n={len(daily_trend)})")

    # 3) 제품별 release 트렌드 생성 (pre 제거)
    meta = products[["product_id", "release_date"]].drop_duplicates("product_id").copy()
    meta["google_trend_release"] = meta["release_date"].apply(lambda d: trend_value_on_date(daily_trend, d))

    # 4) 거래 row 단위 n_day 트렌드 + 가격/사이즈 포함
    base_cols = ["product_id", "trade_date", "current_price"]
    if "size" in trades.columns:
        base_cols.append("size")

    trades_out = trades[base_cols].copy()
    trades_out["google_trend_n_day"] = trades_out["trade_date"].apply(lambda d: trend_value_on_date(daily_trend, d))

    # 5) merge
    out = trades_out.merge(
        meta[["product_id", "google_trend_release"]],
        on="product_id",
        how="left"
    )

    # trade_date 포맷
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["product_id"] = out["product_id"].astype(str)

    # 6) 저장 (덮어쓰기 방식: 매번 깨끗하게)
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"[DONE] 저장 완료: {OUTPUT_CSV}")
    print(f"       저장 행 수: {len(out)}")
    print("\n[SAMPLE]")
    print(out.head(10))
