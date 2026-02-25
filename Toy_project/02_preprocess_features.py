# 02_preprocess_features.py
# Build product-level features from:
# - 01_nike_products.csv (product metadata)
# - 01_nike_trades.csv   (raw trades, all sizes)
#
# Output:
# - 02_features_product_level.csv

import pandas as pd

PRODUCTS_CSV = "01_nike_products.csv"
TRADES_CSV   = "01_nike_trades.csv"
OUT_FEATURES = "02_features_product_level.csv"

GOLDEN_SIZES = {"235", "240", "245", "265", "270", "275"}

# ----------------------
# Load
# ----------------------
products = pd.read_csv(PRODUCTS_CSV, dtype={"product_id": str})
trades   = pd.read_csv(TRADES_CSV,   dtype={"product_id": str, "size": str})

# ----------------------
# Clean trades
# ----------------------
trades["product_id"] = trades["product_id"].astype(str).str.strip()
trades["size"]       = trades["size"].astype(str).str.strip()
trades["price"]      = pd.to_numeric(trades["price"], errors="coerce")

# ✅ date_str 제거됨 → trade_date 기준으로 중복 제거
trades = trades.drop_duplicates(
    subset=["product_id", "size", "price", "trade_date"], keep="first"
)

# Golden subset
golden = trades[trades["size"].isin(GOLDEN_SIZES)].copy()

# ----------------------
# Trade-derived features
# ----------------------
# 전체 거래 수 (전체 사이즈)
trade_count_total = (
    trades.groupby("product_id", as_index=False)
          .size()
          .rename(columns={"size": "trade_count_total"})
)

# 골든사이즈 거래 수
trade_count_golden = (
    golden.groupby("product_id", as_index=False)
          .size()
          .rename(columns={"size": "trade_count_golden"})
)

# 골든사이즈 가중 평균가 (전체 골든 거래 평균)
golden_mean_weighted = (
    golden.groupby("product_id", as_index=False)["price"]
          .mean()
          .rename(columns={"price": "golden_mean_weighted"})
)

# 골든사이즈 비가중 평균가 (사이즈별 평균 → 사이즈 평균)
size_means = (
    golden.groupby(["product_id", "size"], as_index=False)["price"]
          .mean()
          .rename(columns={"price": "avg_price"})
)
pivot = size_means.pivot(index="product_id", columns="size", values="avg_price").reset_index()
for s in GOLDEN_SIZES:
    if s not in pivot.columns:
        pivot[s] = pd.NA
size_cols = sorted(list(GOLDEN_SIZES))
pivot["golden_mean_unweighted"] = pivot[size_cols].mean(axis=1, skipna=True)
golden_mean_unweighted = pivot[["product_id", "golden_mean_unweighted"]].copy()

# ----------------------
# Clean products
# ----------------------
# ✅ 크롤러가 이미 영어 컬럼으로 저장 → rename 불필요
# ✅ 확정 컬럼 기준: is_limited / is_domestic / google_trend_score 제거
# ✅ google_trend 3개는 trade CSV 소속이므로 여기서 집계하지 않음
# ✅ trade_count(크롤러 메타) 제거, trade_count_total로 대체

drop_cols = ["product_name", "model_number", "trade_count", "status", "error", "collected_at"]
products  = products.drop(columns=[c for c in drop_cols if c in products.columns])

# release_price 숫자 변환
if "release_price" in products.columns:
    products["release_price"] = (
        products["release_price"].astype(str)
          .str.replace(",", "", regex=False)
          .str.replace("원", "", regex=False)
    )
    products["release_price"] = pd.to_numeric(products["release_price"], errors="coerce")

# release_date 날짜 변환
if "release_date" in products.columns:
    products["release_date"] = pd.to_datetime(products["release_date"], errors="coerce")

# is_collaboration → int (수기 입력값 정리)
if "is_collaboration" in products.columns:
    products["is_collaboration"] = products["is_collaboration"].fillna(0).astype(int)
else:
    products["is_collaboration"] = 0

# wish_count 숫자 변환
if "wish_count" in products.columns:
    products["wish_count"] = pd.to_numeric(products["wish_count"], errors="coerce")

# 필수 NaN 제거
products = products.dropna(subset=["release_date", "release_price"])
products = products[products["release_price"] > 0]

# product_id 중복 제거 (첫 행 유지)
products = products.drop_duplicates(subset=["product_id"], keep="first")

# ----------------------
# 최종 컬럼 구성
# ----------------------
# ✅ 확정 컬럼 기준 product-level feature
final_product_cols = [
    "product_id",
    "wish_count",
    "release_date",
    "release_price",
    "color",
    "is_collaboration",
]
for c in final_product_cols:
    if c not in products.columns:
        products[c] = pd.NA

features = products[final_product_cols].copy()

# trade-derived feature 병합
features = features.merge(trade_count_total,      on="product_id", how="left")
features = features.merge(trade_count_golden,     on="product_id", how="left")
features = features.merge(golden_mean_unweighted, on="product_id", how="left")
features = features.merge(golden_mean_weighted,   on="product_id", how="left")

# 거래 수 NaN → 0
features["trade_count_total"]  = features["trade_count_total"].fillna(0).astype(int)
features["trade_count_golden"] = features["trade_count_golden"].fillna(0).astype(int)

# ----------------------
# 저장
# ----------------------
features.to_csv(OUT_FEATURES, index=False, encoding="utf-8-sig")
print(f"[완료] 저장: {OUT_FEATURES}")
print(f"       행 수: {len(features):,} | 컬럼: {list(features.columns)}")
