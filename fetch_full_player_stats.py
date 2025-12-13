# fetch_full_player_stats.py
#
# Purpose
# -------
# 1. Snapshot Yahoo cumulative stats ONCE per fantasy day
# 2. Store snapshots in player_stats_daily/YYYY-MM-DD.csv
# 3. Rebuild player_stats_full.parquet by diffing cumulative snapshots
#
# IMPORTANT YAHOO REALITIES
# -------------------------
# - Yahoo returns EMPTY stats if the fantasy day has not closed yet
# - Yahoo does NOT backfill historical daily boxscores
# - Empty snapshot days MUST be handled gracefully
#
# This script is hardened for all of the above.

from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"


# =========================
# Helpers
# =========================

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = find_first(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for i in obj:
            found = find_first(i, key)
            if found is not None:
                return found
    return None


def get_json(oauth: OAuth2, path: str) -> Dict[str, Any]:
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"

    r = oauth.session.get(url)
    if r.status_code != 200:
        return {}

    try:
        return r.json()
    except Exception:
        return {}


def get_snapshot_date_utc() -> str:
    # We label snapshots using UTC date only
    return datetime.now(timezone.utc).date().isoformat()


# =========================
# Yahoo Stats Extraction
# =========================

def extract_cumulative_stats(
    oauth: OAuth2,
    player_key: str,
    snapshot_date: str,
) -> List[Dict[str, Any]]:

    # NOTE:
    # Yahoo only returns data once the fantasy day closes
    rel = f"player/{player_key}/stats;type=date;date={snapshot_date}"
    data = get_json(oauth, rel)

    fc = data.get("fantasy_content", {})
    player = fc.get("player")
    if not player:
        return []

    name_obj = find_first(player, "name")
    player_name = (
        name_obj.get("full")
        if isinstance(name_obj, dict) and "full" in name_obj
        else "Unknown"
    )

    player_stats = find_first(player, "player_stats")
    if not isinstance(player_stats, dict):
        return []

    stats = find_first(player_stats, "stats")
    stat_items = []

    if isinstance(stats, dict) and "stat" in stats:
        stat_items = ensure_list(stats["stat"])
    elif isinstance(stats, list):
        stat_items = stats

    rows = []

    for s in stat_items:
        stat_id = find_first(s, "stat_id")
        value = find_first(s, "value")

        # Yahoo returns "-" for unavailable stats
        if stat_id is None or value in (None, "-", ""):
            continue

        rows.append(
            {
                "player_key": player_key,
                "player_name": player_name,
                "timestamp": snapshot_date,
                "stat_id": str(stat_id),
                "stat_value": value,
            }
        )

    return rows


# =========================
# Parquet Builder
# =========================

def rebuild_full_parquet():
    if not os.path.isdir(DAILY_DIR):
        return

    files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".csv"))
    if not files:
        return

    dfs = []

    for f in files:
        path = os.path.join(DAILY_DIR, f)
        try:
            df = pd.read_csv(path, dtype=str)
            if not df.empty:
                dfs.append(df)
        except pd.errors.EmptyDataError:
            # This happens when Yahoo returned nothing for that day
            print(f"Skipping empty snapshot: {f}")

    if not dfs:
        print("No usable daily snapshots yet.")
        return

    full = pd.concat(dfs, ignore_index=True)

    full["stat_value_num"] = pd.to_numeric(
        full["stat_value"], errors="coerce"
    )

    full.sort_values(
        ["player_key", "stat_id", "timestamp"],
        inplace=True,
        ignore_index=True,
    )

    full["daily_value"] = (
        full.groupby(["player_key", "stat_id"])["stat_value_num"]
        .diff()
        .fillna(full["stat_value_num"])
    )

    full.to_parquet(FULL_PARQUET, index=False)

    print(
        f"✅ Rebuilt {FULL_PARQUET} "
        f"({full['timestamp'].min()} → {full['timestamp'].max()})"
    )


# =========================
# Main
# =========================

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("oauth2.json missing")

    os.makedirs(DAILY_DIR, exist_ok=True)

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    # Use team_rosters.csv as player source (authoritative)
    if not os.path.exists("team_rosters.csv"):
        raise SystemExit("team_rosters.csv missing – run fetch_rosters_and_standings.py first")

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    player_keys = sorted(rosters["player_key"].dropna().unique())

    snapshot_date = get_snapshot_date_utc()
    print(f"Snapshot date (UTC): {snapshot_date}")
    print(f"Fetching stats for {len(player_keys)} players")

    rows = []

    for i, pk in enumerate(player_keys, 1):
        print(f"[{i}/{len(player_keys)}] Fetching {pk}")
        try:
            rows.extend(extract_cumulative_stats(oauth, pk, snapshot_date))
        except Exception as e:
            print(f"Failed for {pk}: {e}")

    daily_path = os.path.join(DAILY_DIR, f"{snapshot_date}.csv")

    # ALWAYS write headers, even if empty
    columns = ["player_key", "player_name", "timestamp", "stat_id", "stat_value"]
    df_day = pd.DataFrame(rows, columns=columns)
    df_day.to_csv(daily_path, index=False)

    print(f"✅ Wrote {len(df_day)} rows → {daily_path}")

    rebuild_full_parquet()


if __name__ == "__main__":
    main()
