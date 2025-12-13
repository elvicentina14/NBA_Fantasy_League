from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"


# ---------------- helpers ----------------

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
            r = find_first(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for i in obj:
            r = find_first(i, key)
            if r is not None:
                return r
    return None


def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"
    resp = oauth.session.get(url)
    resp.raise_for_status()
    return resp.json()


def snapshot_date():
    return datetime.now(timezone.utc).date().isoformat()


# ---------------- stats fetch ----------------

def extract_stats(oauth, player_key, snap):
    data = get_json(oauth, f"player/{player_key}/stats;date={snap}")
    player = find_first(data, "player")
    if not player:
        return []

    name = find_first(player, "full") or "Unknown"
    stats = find_first(player, "stats")

    rows = []
    for s in ensure_list(find_first(stats, "stat")):
        stat_id = find_first(s, "stat_id")
        value = find_first(s, "value")
        if stat_id is None:
            continue

        rows.append({
            "player_key": player_key,
            "player_name": name,
            "timestamp": snap,
            "stat_id": str(stat_id),
            "stat_value": value,
        })

    return rows


# ---------------- parquet builders ----------------

def build_full_parquet():
    files = sorted(
        f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")
    )
    dfs = [pd.read_csv(os.path.join(DAILY_DIR, f), dtype=str) for f in files]
    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    df["stat_value_num"] = pd.to_numeric(df["stat_value"], errors="coerce")

    df.sort_values(
        ["player_key", "stat_id", "timestamp"],
        inplace=True,
        ignore_index=True,
    )

    df["daily_value"] = (
        df.groupby(["player_key", "stat_id"])["stat_value_num"]
        .diff()
        .fillna(df["stat_value_num"])
    )

    df.to_parquet(FULL_PARQUET, index=False)
    return df


# ---------------- main ----------------

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth invalid")

    if not os.path.exists("team_rosters.csv"):
        raise SystemExit("team_rosters.csv missing — run fetch_rosters_and_standings.py first")

    base = pd.read_csv("team_rosters.csv", dtype=str)
    player_keys = sorted(base["player_key"].dropna().unique())

    os.makedirs(DAILY_DIR, exist_ok=True)
    snap = snapshot_date()
    print(f"Snapshot date: {snap}")

    rows = []
    for i, pk in enumerate(player_keys, 1):
        print(f"[{i}/{len(player_keys)}] {pk}")
        rows.extend(extract_stats(oauth, pk, snap))

    daily_path = os.path.join(DAILY_DIR, f"{snap}.csv")
    pd.DataFrame(rows).to_csv(daily_path, index=False)
    print(f"Wrote {len(rows)} rows → {daily_path}")

    full = build_full_parquet()
    if full is not None:
        combined = base.merge(full, on="player_key", how="left")
        combined.to_parquet(COMBINED_PARQUET, index=False)
        print(f"Wrote → {COMBINED_PARQUET}")


if __name__ == "__main__":
    main()
