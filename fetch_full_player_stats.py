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


def ensure_list(x):
    return x if isinstance(x, list) else [x]


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
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/{path}?format=json"
    r = oauth.session.get(url)
    return r.json() if r.status_code == 200 else {}


def main():
    os.makedirs(DAILY_DIR, exist_ok=True)
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)

    league_players = pd.read_csv("team_rosters.csv")[["player_key", "player_name"]].drop_duplicates()

    snapshot = datetime.now(timezone.utc).date().isoformat()
    rows: List[Dict[str, Any]] = []

    for pk in league_players["player_key"]:
        data = get_json(oauth, f"player/{pk}/stats;date={snapshot}")
        player = find_first(data, "player")
        stats = find_first(player, "stats")

        if not stats:
            continue

        for s in ensure_list(stats.get("stat")):
            rows.append({
                "player_key": pk,
                "player_name": find_first(player, "full"),
                "stat_id": find_first(s, "stat_id"),
                "stat_value": find_first(s, "value"),
                "timestamp": snapshot
            })

    daily_df = pd.DataFrame(rows)
    daily_df.to_csv(f"{DAILY_DIR}/{snapshot}.csv", index=False)

    # ---- build parquet ----
    all_days = []
    for f in os.listdir(DAILY_DIR):
        all_days.append(pd.read_csv(f"{DAILY_DIR}/{f}", dtype=str))

    full = pd.concat(all_days)
    full["stat_value_num"] = pd.to_numeric(full["stat_value"], errors="coerce")

    full.sort_values(["player_key", "stat_id", "timestamp"], inplace=True)
    full["daily_value"] = full.groupby(["player_key", "stat_id"])["stat_value_num"].diff()
    full["daily_value"] = full["daily_value"].fillna(full["stat_value_num"])

    full.to_parquet(FULL_PARQUET, index=False)

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    combined = rosters.merge(full, on="player_key", how="left")
    combined.to_parquet(COMBINED_PARQUET, index=False)


if __name__ == "__main__":
    main()
