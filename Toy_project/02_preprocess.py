# 02_preprocess.py
# Outputs:
#  - product_weekly_by_size_plus_wide.csv
#  - product_weekly_by_size_plus_long.csv
#
# 포함 피처:
# 1) 전체(All trades): TotalCnt_All, TotalAvg_All
# 2) 사이즈별(골든 6개): TotalCnt_235, TotalAvg_235 ... TotalCnt_270, TotalAvg_270
# 3) 골든 합산(Golden pooled): TotalCnt_Golden, TotalAvg_Golden
# 4) 주차별(사이즈별): WeekXX_<size>_Avg, WeekXX_<size>_Cnt
# 5) 주차별(골든 합산): WeekXX_Golden_Avg, WeekXX_Golden_Cnt
#
# 기준 주차:
# - product_id별 oldest_trade_date(전체 거래의 min)를 기준으로 0~6일 = Week01

import os
import pandas as pd
import numpy as np


# =========================
# CONFIG
# =========================
PRODUCTS_CSV = "newbalance_products.csv"
TRADES_CSV   = "newbalance_trades.csv"

OUT_WIDE = "product_weekly_by_size_plus_wide.csv"
OUT_LONG = "product_weekly_by_size_plus_long.csv"

GOLDEN_SIZES = ["235", "240", "245", "260", "265", "270"]

# 6개월 = 180일(필요하면 183 등으로 조정 가능)
WINDOW_DAYS = 180
MAX_WEEKS = int(np.ceil(WINDOW_DAYS / 7.0))  # 180 -> 26주

WEEK_NUMBER_START_AT_1 = True  # Week01 = days 0~6


def _safe_read_csv(path: str, dtype=None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    return pd.read_csv(path, dtype=dtype)


def main():
    prod = _safe_read_csv(PRODUCTS_CSV, dtype={"product_id": str})
    tr = _safe_read_csv(TRADES_CSV, dtype={"product_id": str, "size": str})

    # ---- clean ----
    tr["trade_date"] = pd.to_datetime(tr["trade_date"], errors="coerce")
    tr["price"] = pd.to_numeric(tr["price"], errors="coerce")
    tr["size"] = tr["size"].astype(str)

    tr = tr.dropna(subset=["product_id", "trade_date", "price", "size"]).copy()

    # ---- baseline oldest date per product (ALL trades 기준) ----
    baseline = (
        tr.groupby("product_id", as_index=False)["trade_date"]
          .min()
          .rename(columns={"trade_date": "oldest_trade_date"})
    )

    tr = tr.merge(baseline, on="product_id", how="left")
    tr["days_since_oldest"] = (tr["trade_date"] - tr["oldest_trade_date"]).dt.days
    tr = tr[tr["days_since_oldest"] >= 0].copy()

    # 윈도우 제한: oldest 기준 180일까지만
    tr = tr[tr["days_since_oldest"] <= WINDOW_DAYS].copy()

    # week index
    tr["week_idx0"] = (tr["days_since_oldest"] // 7).astype(int)
    tr["week_no"] = tr["week_idx0"] + (1 if WEEK_NUMBER_START_AT_1 else 0)

    # ---- 전체(All) totals ----
    totals_all = (
        tr.groupby("product_id", as_index=False)
          .agg(TotalCnt_All=("price", "size"),
               TotalAvg_All=("price", "mean"))
    )
    totals_all["TotalAvg_All"] = totals_all["TotalAvg_All"].round(0).astype("Int64")
    totals_all["TotalCnt_All"] = totals_all["TotalCnt_All"].astype(int)

    # ---- golden only ----
    tr_g = tr[tr["size"].isin(GOLDEN_SIZES)].copy()

    # ---- size totals (golden sizes only) ----
    totals_by_size = (
        tr_g.groupby(["product_id", "size"], as_index=False)
            .agg(total_cnt=("price", "size"),
                 total_avg=("price", "mean"))
    )
    totals_by_size["total_avg"] = totals_by_size["total_avg"].round(0).astype("Int64")
    totals_by_size["total_cnt"] = totals_by_size["total_cnt"].astype(int)

    # ---- golden pooled totals (골든 6개를 합쳐서) ----
    totals_golden = (
        tr_g.groupby("product_id", as_index=False)
            .agg(TotalCnt_Golden=("price", "size"),
                 TotalAvg_Golden=("price", "mean"))
    )
    totals_golden["TotalAvg_Golden"] = totals_golden["TotalAvg_Golden"].round(0).astype("Int64")
    totals_golden["TotalCnt_Golden"] = totals_golden["TotalCnt_Golden"].astype(int)

    # ---- weekly agg by product + size + week (golden sizes only) ----
    weekly_size = (
        tr_g.groupby(["product_id", "size", "week_no"], as_index=False)
            .agg(Week_Avg=("price", "mean"),
                 Week_Cnt=("price", "size"))
    )
    weekly_size["Week_Avg"] = weekly_size["Week_Avg"].round(0).astype("Int64")
    weekly_size["Week_Cnt"] = weekly_size["Week_Cnt"].astype(int)

    # ---- weekly agg pooled golden (product + week) ----
    weekly_golden = (
        tr_g.groupby(["product_id", "week_no"], as_index=False)
            .agg(Week_Golden_Avg=("price", "mean"),
                 Week_Golden_Cnt=("price", "size"))
    )
    weekly_golden["Week_Golden_Avg"] = weekly_golden["Week_Golden_Avg"].round(0).astype("Int64")
    weekly_golden["Week_Golden_Cnt"] = weekly_golden["Week_Golden_Cnt"].astype(int)

    # =========================
    # LONG output
    # =========================
    # size별 주차 + totals/baseline/all/pooled join
    long_size = (
        weekly_size.merge(baseline, on="product_id", how="left")
                  .merge(totals_all, on="product_id", how="left")
                  .merge(totals_golden, on="product_id", how="left")
                  .merge(totals_by_size, on=["product_id", "size"], how="left")
                  .sort_values(["product_id", "size", "week_no"])
    )

    # pooled golden long도 같이 저장(옵션)
    long_golden = (
        weekly_golden.merge(baseline, on="product_id", how="left")
                    .merge(totals_all, on="product_id", how="left")
                    .merge(totals_golden, on="product_id", how="left")
                    .sort_values(["product_id", "week_no"])
    )
    long_golden["size"] = "GOLDEN_POOLED"
    long_golden = long_golden.rename(columns={
        "Week_Golden_Avg": "Week_Avg",
        "Week_Golden_Cnt": "Week_Cnt"
    })

    long_out = pd.concat([long_size, long_golden], ignore_index=True)
    long_out.to_csv(OUT_LONG, index=False, encoding="utf-8-sig")

    # =========================
    # WIDE output
    # =========================
    # ---- size weekly pivot ----
    avg_wide = weekly_size.pivot(index="product_id", columns=["week_no", "size"], values="Week_Avg")
    cnt_wide = weekly_size.pivot(index="product_id", columns=["week_no", "size"], values="Week_Cnt")

    def colname(week_no, size, suffix):
        return f"Week{int(week_no):02d}_{size}_{suffix}"

    avg_wide.columns = [colname(w, s, "Avg") for (w, s) in avg_wide.columns]
    cnt_wide.columns = [colname(w, s, "Cnt") for (w, s) in cnt_wide.columns]

    wide_size_feat = pd.concat([avg_wide, cnt_wide], axis=1).reset_index()

    # ---- pooled golden weekly pivot ----
    gold_avg = weekly_golden.pivot(index="product_id", columns="week_no", values="Week_Golden_Avg")
    gold_cnt = weekly_golden.pivot(index="product_id", columns="week_no", values="Week_Golden_Cnt")
    gold_avg.columns = [f"Week{int(w):02d}_Golden_Avg" for w in gold_avg.columns]
    gold_cnt.columns = [f"Week{int(w):02d}_Golden_Cnt" for w in gold_cnt.columns]
    wide_golden_feat = pd.concat([gold_avg, gold_cnt], axis=1).reset_index()

    # ---- totals_by_size wide ----
    tot_cnt_w = totals_by_size.pivot(index="product_id", columns="size", values="total_cnt")
    tot_avg_w = totals_by_size.pivot(index="product_id", columns="size", values="total_avg")
    tot_cnt_w.columns = [f"TotalCnt_{s}" for s in tot_cnt_w.columns]
    tot_avg_w.columns = [f"TotalAvg_{s}" for s in tot_avg_w.columns]
    totals_size_wide = pd.concat([tot_cnt_w, tot_avg_w], axis=1).reset_index()

    # ---- 원하는 주차/사이즈 컬럼 고정 생성 ----
    desired_weeks = range(1, MAX_WEEKS + 1) if WEEK_NUMBER_START_AT_1 else range(0, MAX_WEEKS)
    desired_cols = []

    # size별 week cols
    for w in desired_weeks:
        for s in GOLDEN_SIZES:
            desired_cols.append(f"Week{int(w):02d}_{s}_Avg")
            desired_cols.append(f"Week{int(w):02d}_{s}_Cnt")
    # pooled golden week cols
    for w in desired_weeks:
        desired_cols.append(f"Week{int(w):02d}_Golden_Avg")
        desired_cols.append(f"Week{int(w):02d}_Golden_Cnt")

    # wide feature merge
    wide_feat = wide_size_feat.merge(wide_golden_feat, on="product_id", how="outer")

    # 없는 컬럼 강제로 생성
    for c in desired_cols:
        if c not in wide_feat.columns:
            wide_feat[c] = pd.NA

    # ---- meta join ----
    meta_cols = ["product_id"]
    for c in ["한글명", "관심수", "모델번호", "발매일", "발매가", "색상", "is_womens"]:
        if c in prod.columns:
            meta_cols.append(c)

    wide_df = (
        prod[meta_cols].drop_duplicates("product_id")
        .merge(baseline, on="product_id", how="left")
        .merge(totals_all, on="product_id", how="left")
        .merge(totals_golden, on="product_id", how="left")
        .merge(totals_size_wide, on="product_id", how="left")
        .merge(wide_feat[["product_id"] + desired_cols], on="product_id", how="left")
    )

    # totals size cols 고정(없으면 생성)
    tot_size_cols = []
    for s in GOLDEN_SIZES:
        tot_size_cols += [f"TotalCnt_{s}", f"TotalAvg_{s}"]
    for c in tot_size_cols:
        if c not in wide_df.columns:
            wide_df[c] = pd.NA

    # ---- 컬럼 정렬 ----
    front = [c for c in meta_cols if c in wide_df.columns] + ["oldest_trade_date"]
    totals_front = ["TotalCnt_All", "TotalAvg_All", "TotalCnt_Golden", "TotalAvg_Golden"] + tot_size_cols
    for c in totals_front:
        if c not in wide_df.columns:
            wide_df[c] = pd.NA

    wide_df = wide_df[front + totals_front + desired_cols]

    wide_df.to_csv(OUT_WIDE, index=False, encoding="utf-8-sig")

    print("[DONE]")
    print(f"- Long : {OUT_LONG}")
    print(f"- Wide : {OUT_WIDE}")
    print(f"- products rows: {len(prod):,}")
    print(f"- trades valid rows: {len(tr):,}")
    print(f"- golden trades valid rows: {len(tr_g):,}")
    print(f"- unique products in trades: {tr['product_id'].nunique():,}")
    print(f"- unique products in golden trades: {tr_g['product_id'].nunique():,}")


if __name__ == "__main__":
    main()

