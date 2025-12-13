#!/usr/bin/env python3
"""
fetch_full_player_stats.py

Snapshots Yahoo cumulative stats for yesterday (UTC),
writes:
 - player_stats_daily/YYYY-MM-DD.csv
 - player_stats_full.parquet

Relies on team_rosters.csv (produced by rosters script).
"""

from yahoo_oauth import OAuth2
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"


# ---------------- Helpers ---------------- #

def ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_all(obj: Any, key: str, out: List[Dict[str, Any]]):
    """
    Recursively collect all dicts that contain a given key.
    """
    if isinstance(obj, dict):
        if key in obj:
            out.append(obj)
        for v in obj.values():
            find_all(v, key, out)
    elif isinstance(obj, list):
        for item in obj:
            find_all(item, key, out)


def get_json(oauth: OAuth2, path: str) -> Dict[str, Any]:
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if not url.lower().endswith("format=json"):
        url += "?format=json"
    try:
        r = oauth.session.get(url)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"WARNING: fetch error {e} for {url}")
        return {}


def extract_name(node: Dict[str, Any]):
    if not isinstance(node, dict):
        return None
    n = node.get("name")
    if isinstance(n, dict):
        return n.get("full")
    return n


# ---------------- Main ---------------- #

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("oauth2.json missing")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    if not os.path.exists("team_rosters.csv"):
        raise SystemExit("team_rosters.csv missing — run rosters script first")

    # Use **yesterday** (UTC) as snapshot label
    snapshot_date = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    print("Snapshot date (UTC-1):", snapshot_date)

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    player_keys = sorted(rosters["player_key"].dropna().unique().tolist())
    print(f"Fetching stats for {len(player_keys)} players")

    os.makedirs(DAILY_DIR, exist_ok=True)
    daily_rows = []

    for idx, pk in enumerate(player_keys, start=1):
        print(f"[{idx}/{len(player_keys)}] {pk}")
        data = get_json(oauth, f"player/{pk}/stats;type=date;date={snapshot_date}")

        # collect all stat dicts
        stat_nodes = []
        find_all(data, "stat", stat_nodes)

        # extract name from each player node inside
        pname = extract_name(data)

        for s in stat_nodes:
            if not isinstance(s, dict):
                continue

            stat_id = s.get("stat_id")
            value = s.get("value")

            # skip empty / Yahoo '-' values
            if stat_id is None or value in (None, "-", ""):
                continue

            daily_rows.append({
                "player_key": pk,
                "player_name": pname,
                "timestamp": snapshot_date,
                "stat_id": str(stat_id),
                "stat_value": value
            })

    # Write daily CSV (headers even if empty)
    cols = ["player_key","player_name","timestamp","stat_id","stat_value"]
    df_day = pd.DataFrame(daily_rows, columns=cols)
    daily_path = os.path.join(DAILY_DIR, f"{snapshot_date}.csv")
    df_day.to_csv(daily_path, index=False)
    print(f"Saved {len(df_day)} rows → {daily_path}")

    # Build full parquet
    all_csvs = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".csv"))
    frames = []
    for f in all_csvs:
        path = os.path.join(DAILY_DIR, f)
        try:
            df = pd.read_csv(path, dtype=str)
            if not df.empty:
                frames.append(df)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty CSV: {f}")

    if not frames:
        print("No stats data yet, skipping parquet build.")
        return

    full = pd.concat(frames, ignore_index=True)
    full["stat_value_num"] = pd.to_numeric(full["stat_value"], errors="coerce")
    full.sort_values(["player_key","stat_id","timestamp"], inplace=True)
    full["daily_value"] = full.groupby(["player_key","stat_id"])["stat_value_num"].diff()
    full["daily_value"] = full["daily_value"].fillna(full["stat_value_num"])

    full.to_parquet(FULL_PARQUET, index=False)
    print(f"Saved full stats to {FULL_PARQUET}")


if __name__ == "__main__":
    main()
