# rebuild_from_daily.py
#
# Rebuilds:
#   - player_stats_full.parquet
#   - combined_player_view_full.parquet
# using ONLY the CSVs in player_stats_daily/
#
# Requirements:
#   - player_stats_daily/*.csv exists (2025 season)
#   - league_players.csv in repo root (for combined view)

import os
from typing import List

import pandas as pd

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"

# --- 1. Build player_stats_full.parquet from daily CSVs --- #

def build_full_parquet_from_daily() -> pd.DataFrame | None:
    if not os.path.isdir(DAILY_DIR):
        print(f"[ERROR] {DAILY_DIR} directory not found.")
        return None

    files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".csv"))
    if not files:
        print(f"[ERROR] No CSV files found in {DAILY_DIR}.")
        return None

    print(f"Found {len(files)} daily CSV files in {DAILY_DIR}:")
    for f in files:
        print("  -", f)

    dfs: List[pd.DataFrame] = []
    for fname in files:
        path = os.path.join(DAILY_DIR, fname)
        try:
            df = pd.read_csv(path, dtype=str)
            if df.empty:
                print(f"[WARN] {fname} is empty, skipping.")
                continue
            # Ensure the timestamp column exists and matches the filename
            if "timestamp" not in df.columns:
                # Derive from filename YYYY-MM-DD.csv
                ts = os.path.splitext(fname)[0]
                df["timestamp"] = ts
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Failed to read {path}: {type(e).__name__} {e}")

    if not dfs:
        print("[ERROR] All daily CSVs were empty or unreadable.")
        return None

    full_df = pd.concat(dfs, ignore_index=True)

    # Keep only columns we care about; anything extra is kept but not required
    # Expect at least: player_key, player_name, stat_id, timestamp, stat_value
    core_cols = ["player_key", "player_name", "stat_id", "timestamp", "stat_value"]
    for col in core_cols:
        if col not in full_df.columns:
            full_df[col] = None

    # Force string for keys / ids / timestamps
    for col in ["player_key", "player_name", "stat_id", "timestamp"]:
        full_df[col] = full_df[col].astype(str)

    # Numeric cumulative value
    full_df["stat_value_num"] = pd.to_numeric(full_df["stat_value"], errors="coerce")

    # Sort for correct diff computation
    full_df.sort_values(
        by=["player_key", "stat_id", "timestamp"],
        inplace=True,
        ignore_index=True,
    )

    # daily_value = today's cumulative minus previous day's cumulative
    full_df["daily_value"] = full_df.groupby(
        ["player_key", "stat_id"]
    )["stat_value_num"].diff()

    # For first date of each player+stat, daily_value = stat_value_num
    full_df["daily_value"] = full_df["daily_value"].fillna(full_df["stat_value_num"])

    # OPTIONAL: filter out old seasons if any 2024 stuff is still in there
    # Keep from 2025-10-21 onwards only:
    # full_df = full_df[full_df["timestamp"] >= "2025-10-21"].copy()

    full_df.to_parquet(FULL_PARQUET, index=False)

    ts = full_df["timestamp"].dropna().astype(str)
    print(
        f"[OK] Saved {len(full_df)} rows to {FULL_PARQUET}. "
        f"Dates: {ts.min()} → {ts.max()} ({ts.nunique()} distinct days)"
    )

    return full_df


# --- 2. Build combined_player_view_full.parquet by joining league_players --- #

def build_combined_parquet(full_stats_df: pd.DataFrame) -> None:
    if full_stats_df is None or full_stats_df.empty:
        print("[WARN] full_stats_df is empty, skipping combined parquet.")
        return

    if not os.path.exists("league_players.csv"):
        print("[ERROR] league_players.csv not found in repo root.")
        return

    lp = pd.read_csv("league_players.csv", dtype=str)
    if "player_key" not in lp.columns:
        print("[ERROR] league_players.csv missing 'player_key' column.")
        return

    # Ensure key columns are strings
    for col in ["player_key", "player_name"]:
        if col in lp.columns:
            lp[col] = lp[col].astype(str)
        if col in full_stats_df.columns:
            full_stats_df[col] = full_stats_df[col].astype(str)

    # Some leagues might have missing player_name in either side; merge safely
    if "player_name" in lp.columns and "player_name" in full_stats_df.columns:
        merged = lp.merge(
            full_stats_df,
            on=["player_key", "player_name"],
            how="left",
            validate="m:m",
        )
    else:
        merged = lp.merge(
            full_stats_df,
            on=["player_key"],
            how="left",
            validate="m:m",
        )

    merged.to_parquet(COMBINED_PARQUET, index=False)

    if "timestamp" in merged.columns:
        ts = merged["timestamp"].dropna().astype(str)
        if not ts.empty:
            print(
                f"[OK] Saved {len(merged)} rows to {COMBINED_PARQUET}. "
                f"Dates: {ts.min()} → {ts.max()} ({ts.nunique()} distinct days)"
            )
        else:
            print(
                f"[OK] Saved {len(merged)} rows to {COMBINED_PARQUET}, "
                "but timestamp column is empty."
            )
    else:
        print(
            f"[OK] Saved {len(merged)} rows to {COMBINED_PARQUET}, "
            "no timestamp column present."
        )


def main():
    full_df = build_full_parquet_from_daily()
    if full_df is not None:
        build_combined_parquet(full_df)


if __name__ == "__main__":
    main()
