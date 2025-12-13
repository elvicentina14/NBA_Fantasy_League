from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"


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
    r = oauth.session.get(url)
    return r.json() if r.status_code == 200 else {}


def main():
    os.makedirs(DAILY_DIR, exist_ok=True)

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)

    league_players = pd.read_csv("league_players.csv", dtype=str)
    players = league_players["player_key"].dropna().unique()

    date = datetime.now(timezone.utc).date().isoformat()
    rows = []

    for pk in players:
        data = get_json(oauth, f"player/{pk}/stats;date={date}")
        stats = find_first(data, "stats")

        stat_items = []
        if isinstance(stats, dict):
            stat_items = ensure_list(stats.get("stat"))
        elif isinstance(stats, list):
            for s in stats:
                if isinstance(s, dict) and "stat" in s:
                    stat_items.append(s["stat"])

        for s in stat_items:
            rows.append({
                "player_key": pk,
                "player_name": find_first(s, "name"),
                "stat_id": find_first(s, "stat_id"),
                "stat_value": find_first(s, "value"),
                "timestamp": date
            })

    df_day = pd.DataFrame(rows)
    df_day.to_csv(f"{DAILY_DIR}/{date}.csv", index=False)

    full = pd.concat(
        [pd.read_csv(f"{DAILY_DIR}/{f}") for f in os.listdir(DAILY_DIR)],
        ignore_index=True
    )

    full["stat_value_num"] = pd.to_numeric(full["stat_value"], errors="coerce")
    full.sort_values(["player_key", "stat_id", "timestamp"], inplace=True)
    full["daily_value"] = full.groupby(["player_key", "stat_id"])["stat_value_num"].diff().fillna(full["stat_value_num"])

    full.to_parquet(FULL_PARQUET, index=False)

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    combined = rosters.merge(full, on="player_key", how="left")
    combined.to_parquet(COMBINED_PARQUET, index=False)


if __name__ == "__main__":
    main()
