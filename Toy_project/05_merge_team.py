# 04_merge_team.py

import pandas as pd
import warnings

# ════════════════════════════════════════════════
# 파일 경로 설정 — 여기서만 수정하면 됩니다
# ════════════════════════════════════════════════

INPUT_FILES = [
    r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\04_preprocessed_1-20.csv",
    r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\04_preprocessed_21-40.csv",
    r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\04_preprocessed_41-60.csv",
    r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\04_preprocessed_61-80.csv",
    r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\04_preprocessed_81-100.csv",
    r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\04_preprocessed_101-120.csv",
]

OUTPUT_FILE = r"C:\Users\lg\min_python\KHUDA\KHUDA_9_ML\Toy_project\outputs\05_merged_dataset.csv"

# ════════════════════════════════════════════════


if __name__ == "__main__":
    dfs = []

    for path in INPUT_FILES:
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
            print(f"  ✅ 읽기 성공: {path.split(chr(92))[-1]}  ({len(df):,}행)")
            dfs.append(df)
        except FileNotFoundError:
            warnings.warn(f"[병합] 파일 없음, 건너뜀: {path}", UserWarning)
            print(f"  ⚠️  파일 없음, 건너뜀: {path.split(chr(92))[-1]}")

    if not dfs:
        print("\n❌ 읽을 수 있는 파일이 없습니다. 경로를 확인해주세요.")
    else:
        merged = pd.concat(dfs, ignore_index=True)

        # ── 저장 ──
        merged.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

        print(f"\n{'='*50}")
        print(f"  병합 완료!")
        print(f"  파일 수     : {len(dfs)}개")
        print(f"  총 행 수    : {len(merged):,}행")
        print(f"  총 제품 수  : {merged['product_id'].nunique()}개")
        print(f"  저장 위치   : {OUTPUT_FILE}")
        print(f"{'='*50}")