# fetch_full_player_stats.py
#
# Robust Yahoo Fantasy NBA stats collector
# - Handles Yahoo list/dict chaos safely
# - Builds daily CSV snapshots
# - Builds full Parquet
# - Joins with team_rosters.csv

from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"


# ---------------- helpers ---------------- #

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def deep_find(obj, key):
    """Return ALL dicts containing `key` anywhere in nested structure."""
    found = []

    if isinstance(obj, dict):
        if key in obj:
            found.append(obj)
        for v in obj.values():
            found.extend(deep_find(v, key))

    elif isinstance(obj, list):
        for item in obj:
            found.extend(deep_find(item, key))

    return found


def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"

    resp = oauth.session.get(url)
    resp.raise_for_status()
    return resp.json()


def extract_name(node):
    if not isinstance(node, dict):
        return None
    name = node.get("name")
    if isinstance(name, dict):
        return name.get("full")
    return None


# ---------------- main ---------------- #

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    os.makedirs(DAILY_DIR, exist_ok=True)

    # ----------- LOAD PLAYERS (FROM ROSTERS) ----------- #

    if not os.path.exists("team_rosters.csv"):
        raise SystemExit("team_rosters.csv not found — run fetch_rosters_and_standings.py first")

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    player_keys = sorted(rosters["player_key"].dropna().unique().tolist())

    print(f"Found {len(player_keys)} players from rosters")

    # ----------- SNAPSHOT DATE ----------- #

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    print(f"Snapshot date: {snapshot_date}")

    daily_rows = []

    # ----------- FETCH STATS ----------- #

    for idx, player_key in enumerate(player_keys, 1):
        print(f"[{idx}/{len(player_keys)}] Fetching stats for {player_key}")

        try:
            data = get_json(
                oauth,
                f"player/{player_key}/stats;type=date;date={snapshot_date}"
            )
        except Exception as e:
            print(f"  ❌ API error: {e}")
            continue

        player_nodes = deep_find(data, "player_key")

        for p in player_nodes:
            if not isinstance(p, dict):
                continue
            if p.get("player_key") != player_key:
                continue

            player_name = extract_name(p)

            stats_nodes = deep_find(p, "stat")

            for s in stats_nodes:
                if not isinstance(s, dict):
                    continue

                stat_id = s.get("stat_id")
                stat_value = s.get("value")

                if stat_id is None:
                    continue

                daily_rows.append(
                    {
                        "player_key": player_key,
                        "player_name": player_name,
                        "timestamp": snapshot_date,
                        "stat_id": str(stat_id),
                        "stat_value": stat_value,
                    }
                )

    # ----------- WRITE DAILY CSV ----------- #

    daily_path = os.path.join(DAILY_DIR, f"{snapshot_date}.csv")
    df_day = pd.DataFrame(daily_rows)
    df_day.to_csv(daily_path, index=False)
    print(f"✅ Wrote {len(df_day)} rows → {daily_path}")

    # ----------- BUILD FULL PARQUET ----------- #

    dfs = []
    for f in sorted(os.listdir(DAILY_DIR)):
        if f.endswith(".csv"):
            dfs.append(pd.read_csv(os.path.join(DAILY_DIR, f), dtype=str))

    if not dfs:
        print("No daily data — skipping parquet build")
        return

    full_df = pd.concat(dfs, ignore_index=True)
    full_df["stat_value_num"] = pd.to_numeric(full_df["stat_value"], errors="coerce")

    full_df.sort_values(
        ["player_key", "stat_id", "timestamp"],
        inplace=True,
        ignore_index=True,
    )

    full_df["daily_value"] = full_df.groupby(
        ["player_key", "stat_id"]
    )["stat_value_num"].diff()

    full_df["daily_value"] = full_df["daily_value"].fillna(full_df["stat_value_num"])

    full_df.to_parquet(FULL_PARQUET, index=False)
    print(f"✅ Wrote {len(full_df)} rows → {FULL_PARQUET}")

    # ----------- COMBINED VIEW ----------- #

    combined = rosters.merge(
        full_df,
        on="player_key",
        how="left",
        validate="m:m",
    )

    combined.to_parquet(COMBINED_PARQUET, index=False)
    print(f"✅ Wrote {len(combined)} rows → {COMBINED_PARQUET}")


if __name__ == "__main__":
    main()
