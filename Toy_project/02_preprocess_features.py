# 02_preprocess_features.py
# Build product-level features from:
# - nike_products.csv (product metadata, includes trade_count from crawler)
# - nike_trades.csv   (raw trades up to 2000, all sizes)
#
# Output:
# - features_product_level.csv
#
# Requirements applied:
# - retail_price -> release_price (column name)
# - keep only trade_count (drop crawl meta like status/error/collected_at)
# - trade-derived features: trade_count_total, trade_count_golden,
#   golden_mean_unweighted, golden_mean_weighted
# - week/labeling calculations are NOT done (only placeholder columns exist if present in products)

import pandas as pd

PRODUCTS_CSV = "01_nike_products.csv"
TRADES_CSV   = "01_nike_trades.csv"
OUT_FEATURES = "02_features_product_level.csv"

GOLDEN_SIZES = {"235", "240", "245", "265", "270", "275"}

# ----------------------
# Load
# ----------------------
products = pd.read_csv(PRODUCTS_CSV, dtype={"product_id": str})
trades = pd.read_csv(TRADES_CSV, dtype={"product_id": str, "size": str})

# ----------------------
# Clean trades
# ----------------------
trades["product_id"] = trades["product_id"].astype(str).str.strip()
trades["size"] = trades["size"].astype(str).str.strip()
trades["price"] = pd.to_numeric(trades["price"], errors="coerce")

# Keep only exact duplicates removal (avoid over-dedup that can undercount)
# If you suspect scroll duplicates are heavy, this is still a safe level.
trades = trades.drop_duplicates(subset=["product_id", "size", "price", "date_str"], keep="first")

# Golden subset
golden = trades[trades["size"].isin(GOLDEN_SIZES)].copy()

# ----------------------
# Trade-derived features
# ----------------------
# Total trade count (all sizes, up to 2000)
trade_count_total = (
    trades.groupby("product_id", as_index=False)
    .size()
    .rename(columns={"size": "trade_count_total"})
)

# Golden trade count (golden sizes only)
trade_count_golden = (
    golden.groupby("product_id", as_index=False)
    .size()
    .rename(columns={"size": "trade_count_golden"})
)

# Golden weighted mean (all golden trades mean)
golden_mean_weighted = (
    golden.groupby("product_id", as_index=False)["price"]
    .mean()
    .rename(columns={"price": "golden_mean_weighted"})
)

# Golden unweighted mean:
# 1) mean price per size
# 2) average those size means (simple average across sizes, ignoring missing)
size_means = (
    golden.groupby(["product_id", "size"], as_index=False)["price"]
    .mean()
    .rename(columns={"price": "avg_price"})
)

pivot = size_means.pivot(index="product_id", columns="size", values="avg_price").reset_index()

# Ensure all six sizes exist as columns (so mean uses consistent set)
for s in GOLDEN_SIZES:
    if s not in pivot.columns:
        pivot[s] = pd.NA

size_cols = sorted(list(GOLDEN_SIZES))  # ["235","240","245","265","270","275"]
pivot["golden_mean_unweighted"] = pivot[size_cols].mean(axis=1, skipna=True)

golden_mean_unweighted = pivot[["product_id", "golden_mean_unweighted"]].copy()

# ----------------------
# Build final feature table
# ----------------------
# Rename product columns to English (robust to missing optional columns)
rename_map = {
    "name": "product_name",
    "모델번호": "model_number",
    "관심수": "wish_count",         # in case older file
    "wish_count": "wish_count",
    "발매일": "release_date",
    "발매가": "release_price",
    "retail_price": "release_price", # safety
    "색상": "color",
    "한정판 여부": "is_limited",
    "국내발매 여부": "is_domestic",
    "콜라보 여부": "is_collaboration",
    "구글트랜드 분석 결과": "google_trend_score",
    "라벨링": "label",
}

products = products.rename(columns={k: v for k, v in rename_map.items() if k in products.columns})

# Keep only required product columns + trade_count
required_product_cols = [
    "product_id",
    "product_name",
    "model_number",
    "wish_count",
    "release_date",
    "release_price",
    "color",
    "is_limited",
    "is_domestic",
    "is_collaboration",
    "google_trend_score",
    "label",
    "trade_count",  # keep only this meta
]

# Create missing required columns as NA (to avoid KeyError)
for c in required_product_cols:
    if c not in products.columns:
        products[c] = pd.NA

features = products[required_product_cols].copy()

# Merge trade-derived features
features = features.merge(trade_count_total, on="product_id", how="left")
features = features.merge(trade_count_golden, on="product_id", how="left")
features = features.merge(golden_mean_unweighted, on="product_id", how="left")
features = features.merge(golden_mean_weighted, on="product_id", how="left")

# Fill counts with 0 if no trades
features["trade_count_total"] = features["trade_count_total"].fillna(0).astype(int)
features["trade_count_golden"] = features["trade_count_golden"].fillna(0).astype(int)

# Save
features.to_csv(OUT_FEATURES, index=False, encoding="utf-8-sig")
print("Saved:", OUT_FEATURES)
print("Columns:", list(features.columns))
