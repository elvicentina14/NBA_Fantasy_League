import os
import pandas as pd
from datetime import datetime, timezone
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
CONFIG = "oauth2.json"

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"

def as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for i in obj:
            r = find(i, key)
            if r is not None:
                return r
    return None

def get(oauth, path):
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/{path}?format=json"
    return oauth.session.get(url).json()

def main():
    oauth = OAuth2(None, None, from_file=CONFIG)
    os.makedirs(DAILY_DIR, exist_ok=True)

    snapshot = datetime.now(timezone.utc).date().isoformat()
    print(f"Snapshot date: {snapshot}")

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    player_keys = sorted(rosters["player_key"].dropna().unique())

    rows = []

    for i, pk in enumerate(player_keys, 1):
        print(f"[{i}/{len(player_keys)}] Fetching stats for {pk}")
        data = get(oauth, f"player/{pk}/stats")

        stats = find(data, "stat")
        for s in as_list(stats):
            stat_id = find(s, "stat_id")
            val = find(s, "value")
            if stat_id is None or val in (None, "-"):
                continue

            rows.append({
                "player_key": pk,
                "player_name": find(data, "full"),
                "timestamp": snapshot,
                "stat_id": stat_id,
                "stat_value": val
            })

    df_day = pd.DataFrame(rows)
    path = f"{DAILY_DIR}/{snapshot}.csv"
    df_day.to_csv(path, index=False)
    print(f"✅ Wrote {len(df_day)} rows → {path}")

    # Build full parquet safely
    dfs = []
    for f in os.listdir(DAILY_DIR):
        p = os.path.join(DAILY_DIR, f)
        if os.path.getsize(p) > 0:
            dfs.append(pd.read_csv(p, dtype=str))

    if not dfs:
        print("No data for parquet build")
        return

    full = pd.concat(dfs, ignore_index=True)
    full["stat_value_num"] = pd.to_numeric(full["stat_value"], errors="coerce")

    full.sort_values(["player_key", "stat_id", "timestamp"], inplace=True)
    full["daily_value"] = full.groupby(["player_key", "stat_id"])["stat_value_num"].diff()
    full["daily_value"].fillna(full["stat_value_num"], inplace=True)

    full.to_parquet(FULL_PARQUET, index=False)

    combined = rosters.merge(full, on="player_key", how="left")
    combined.to_parquet(COMBINED_PARQUET, index=False)

    print("✅ Parquets rebuilt")

if __name__ == "__main__":
    main()
