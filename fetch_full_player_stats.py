from yahoo_oauth import OAuth2
import os
from datetime import datetime, timezone
import pandas as pd

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"
SEASON_START = "2025-10-21"

def get_today():
    return datetime.now(timezone.utc).date().isoformat()

def get_json(oauth, rel):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + rel
    if "format=json" not in url:
        url += ("&" if "?" in url else "?") + "format=json"
    r = oauth.session.get(url)
    try:
        return r.json()
    except:
        return {}

def collect_player_stats(oauth, pk, date):
    d = get_json(oauth, f"player/{pk}/stats;date={date}")
    stats = []
    p = d.get("fantasy_content", {}).get("player", {})
    ps = p.get("player_stats", {})
    stat_list = ps.get("stats", {}).get("stat", [])
    if not isinstance(stat_list, list):
        stat_list = [stat_list]

    name = None
    n = p.get("name", {})
    if "full" in n:
        name = n["full"]
    if not name:
        name = pk

    for s in stat_list:
        stats.append({
            "player_key": pk,
            "player_name": name,
            "timestamp": date,
            "stat_id": s.get("stat", {}).get("stat_id"),
            "stat_value": s.get("stat", {}).get("value")
        })
    return stats

def main():
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        print("Token expired, refresh locally.")
        exit(1)

    os.makedirs(DAILY_DIR, exist_ok=True)

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    pks = sorted(rosters["player_key"].unique())

    today = get_today()
    daily_file = f"{DAILY_DIR}/{today}.csv"

    rows = []
    for i, pk in enumerate(pks, 1):
        print(f"[{i}/{len(pks)}] {pk}")
        rows.extend(collect_player_stats(oauth, pk, today))

    pd.DataFrame(rows).to_csv(daily_file, index=False)
    print(f"Saved -> {daily_file}")

    # rebuild parquet
    dfs = []
    for f in sorted(os.listdir(DAILY_DIR)):
        if not f.endswith(".csv"):
            continue
        df = pd.read_csv(f"{DAILY_DIR}/{f}", dtype=str)
        df["stat_value_num"] = pd.to_numeric(df["stat_value"], errors="coerce")
        dfs.append(df)

    full = pd.concat(dfs)
    full = full[full["timestamp"] >= SEASON_START].copy()

    full.sort_values(by=["player_key", "stat_id", "timestamp"], inplace=True)
    full["daily_value"] = full.groupby(
        ["player_key", "stat_id"]
    )["stat_value_num"].diff().fillna(full["stat_value_num"])

    full.to_parquet(FULL_PARQUET, index=False)

    merged = rosters.merge(full, on="player_key", how="left")
    merged.to_parquet(COMBINED_PARQUET, index=False)

    print(f"Wrote -> {FULL_PARQUET}")
    print(f"Wrote -> {COMBINED_PARQUET}")

if __name__ == "__main__":
    main()
