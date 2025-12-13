from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Any, List, Dict

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"


# ---------- helpers ----------

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj: Any, key: str):
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


def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url += f"{sep}format=json"
    r = oauth.session.get(url)
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}


# ---------- main ----------

def main():
    if not os.path.exists("league_players.csv"):
        raise SystemExit("league_players.csv missing")

    os.makedirs(DAILY_DIR, exist_ok=True)

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    snapshot_date = datetime.now(timezone.utc).date().isoformat()

    players = pd.read_csv("league_players.csv", dtype=str)
    rows = []

    for pk in players["player_key"].dropna().unique():
        data = get_json(oauth, f"player/{pk}/stats;type=date;date={snapshot_date}")
        player = find_first(data, "player")
        if not player:
            continue

        name = find_first(player, "name")
        player_name = name.get("full") if isinstance(name, dict) else name

        stats = find_first(player, "stats")
        stat_items = []

        if isinstance(stats, dict):
            stat_items = ensure_list(stats.get("stat"))
        elif isinstance(stats, list):
            stat_items = stats

        for s in stat_items:
            if not isinstance(s, dict):
                continue
            stat_id = find_first(s, "stat_id")
            value = find_first(s, "value")
            if stat_id is None:
                continue

            rows.append({
                "player_key": pk,
                "player_name": player_name,
                "timestamp": snapshot_date,
                "stat_id": stat_id,
                "stat_value": value
            })

    daily_path = f"{DAILY_DIR}/{snapshot_date}.csv"
    pd.DataFrame(rows).to_csv(daily_path, index=False)
    print(f"Wrote {daily_path}")

    # ---------- build parquet ----------

    dfs = []
    for f in os.listdir(DAILY_DIR):
        if f.endswith(".csv"):
            dfs.append(pd.read_csv(f"{DAILY_DIR}/{f}", dtype=str))

    full = pd.concat(dfs, ignore_index=True)
    full["stat_value_num"] = pd.to_numeric(full["stat_value"], errors="coerce")
    full.sort_values(["player_key", "stat_id", "timestamp"], inplace=True)

    full["daily_value"] = full.groupby(
        ["player_key", "stat_id"]
    )["stat_value_num"].diff().fillna(full["stat_value_num"])

    full.to_parquet(FULL_PARQUET, index=False)

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    combined = rosters.merge(full, on="player_key", how="left")
    combined.to_parquet(COMBINED_PARQUET, index=False)

    print("Parquet files written")


if __name__ == "__main__":
    main()
