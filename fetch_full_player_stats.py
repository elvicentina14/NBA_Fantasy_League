# fetch_full_player_stats.py
from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone, timedelta

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"
LEGACY_CSV = "player_stats_full.csv"


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
        for item in obj:
            found = find_first(item, key)
            if found is not None:
                return found
    return None


def get_json(oauth, relative_path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + relative_path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}format=json"
    resp = oauth.session.get(url)
    resp.raise_for_status()
    return resp.json()


def get_league_current_date(oauth):
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    try:
        data = get_json(oauth, f"league/{LEAGUE_KEY}")
        current_date = find_first(data, "current_date")
        if isinstance(current_date, str) and len(current_date) == 10:
            return current_date
    except:
        pass

    return datetime.now(timezone.utc).date().isoformat()


def get_league_start_date(oauth):
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    try:
        data = get_json(oauth, f"league/{LEAGUE_KEY}")
        start_date = find_first(data, "start_date")
        if isinstance(start_date, str) and len(start_date) == 10:
            return start_date
    except:
        pass

    return get_league_current_date(oauth)


def date_range_iso(start_date_str, end_date_str):
    start = datetime.fromisoformat(start_date_str).date()
    end = datetime.fromisoformat(end_date_str).date()
    d = start
    while d <= end:
        yield d.isoformat()
        d += timedelta(days=1)


def extract_daily_stats_for_player(oauth, player_key, stats_date):
    rel = f"player/{player_key}/stats;date={stats_date}"
    data = get_json(oauth, rel)

    fc = data.get("fantasy_content", {})
    player_node = fc.get("player")

    if player_node is None:
        return []

    name_obj = find_first(player_node, "name")
    if isinstance(name_obj, dict) and "full" in name_obj:
        player_name = name_obj["full"]
    else:
        player_name = find_first(name_obj, "full") if isinstance(name_obj, dict) else None

    if not player_name:
        player_name = "Unknown"

    player_stats = find_first(player_node, "player_stats")
    if not isinstance(player_stats, dict):
        return []

    coverage_type = player_stats.get("coverage_type") or find_first(player_stats, "coverage_type")

    period = (
        player_stats.get("date")
        or player_stats.get("season")
        or player_stats.get("week")
        or stats_date
    )

    stats_node = player_stats.get("stats") or find_first(player_stats, "stats")
    if stats_node is None:
        return []

    stat_items = []
    if isinstance(stats_node, dict):
        if "stat" in stats_node:
            stat_items = ensure_list(stats_node["stat"])
        else:
            stat_items = [stats_node]
    elif isinstance(stats_node, list):
        stat_items = stats_node

    rows = []
    for item in stat_items:
        if isinstance(item, dict) and "stat" in item:
            s = item["stat"]
        else:
            s = item

        stat_id = find_first(s, "stat_id")
        value = find_first(s, "value")
        if stat_id is None:
            continue

        rows.append(
            {
                "player_key": str(player_key),
                "player_name": player_name,
                "coverage": coverage_type or "date",
                "period": period,
                "timestamp": stats_date,
                "stat_id": str(stat_id),
                "stat_value": value,
            }
        )

    return rows


def build_full_parquet_from_daily_and_legacy():
    dfs = []

    # DAILY FOLDER
    if os.path.isdir(DAILY_DIR):
        files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".csv"))
        for fname in files:
            path = os.path.join(DAILY_DIR, fname)
            try:
                df = pd.read_csv(path, dtype=str)
                dfs.append(df)
            except:
                pass

    # LEGACY CSV
    if os.path.exists(LEGACY_CSV):
        try:
            old = pd.read_csv(LEGACY_CSV, dtype=str)
            dfs.append(old)
        except:
            pass

    if not dfs:
        print("NO DATA FOUND FOR FULL PARQUET")
        return None

    full_df = pd.concat(dfs, ignore_index=True)

    # DE-DUPLICATE
    full_df.drop_duplicates(
        subset=["player_key", "timestamp", "stat_id"],
        inplace=True
    )

    # SORT
    full_df.sort_values(
        by=["timestamp", "player_key", "stat_id"],
        inplace=True,
        ignore_index=True
    )

    full_df.to_parquet(FULL_PARQUET, index=False)
    print(f"Saved {len(full_df)} rows to {FULL_PARQUET}")
    return full_df


def build_combined_parquet(base_for_combined, full_stats_df):
    base = base_for_combined.copy()
    for col in ["player_key", "player_name"]:
        if col not in base.columns:
            base[col] = base_for_combined.get(col)

    merged = base.merge(
        full_stats_df,
        on=["player_key", "player_name"],
        how="left",
        validate="m:m",
    )

    merged.to_parquet(COMBINED_PARQUET, index=False)
    print(f"Saved {len(merged)} rows to {COMBINED_PARQUET}")


def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"{CONFIG_FILE} not found")

    os.makedirs(DAILY_DIR, exist_ok=True)

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid. Refresh locally first.")

    if not os.path.exists("league_players.csv"):
        raise SystemExit("league_players.csv missing")

    league_players = pd.read_csv("league_players.csv", dtype=str)

    base_for_combined = league_players.copy()
    if os.path.exists("team
