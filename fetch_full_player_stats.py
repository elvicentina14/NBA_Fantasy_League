# fetch_full_player_stats.py
from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone, timedelta

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"  # folder for daily CSVs
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"


def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj, key):
    """Recursively find the first value for a given key in nested dict/list."""
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
    """Call Yahoo Fantasy API with ?format=json appended."""
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + relative_path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}format=json"
    resp = oauth.session.get(url)
    resp.raise_for_status()
    return resp.json()


def get_league_current_date(oauth):
    """
    Ask Yahoo what the league's current_date is (YYYY-MM-DD).
    If that fails, fall back to today's UTC date.
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    try:
        data = get_json(oauth, f"league/{LEAGUE_KEY}")
        current_date = find_first(data, "current_date")
        if isinstance(current_date, str) and len(current_date) == 10:
            print(f"Using league current_date from Yahoo: {current_date}")
            return current_date
    except Exception as e:
        print("Could not get league current_date from API:", type(e).__name__, e)

    # Fallback: today in UTC
    today_str = datetime.now(timezone.utc).date().isoformat()
    print(f"Falling back to today's UTC date: {today_str}")
    return today_str


def get_league_start_date(oauth):
    """
    Ask Yahoo what the league's start_date is (YYYY-MM-DD).
    If that fails, fall back to current_date.
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    try:
        data = get_json(oauth, f"league/{LEAGUE_KEY}")
        start_date = find_first(data, "start_date")
        if isinstance(start_date, str) and len(start_date) == 10:
            print(f"Using league start_date from Yahoo: {start_date}")
            return start_date
        else:
            print("No start_date found in league settings; falling back to current_date.")
            return get_league_current_date(oauth)
    except Exception as e:
        print("Could not get league start_date from API:", type(e).__name__, e)
        return get_league_current_date(oauth)


def date_range_iso(start_date_str, end_date_str):
    """Yield YYYY-MM-DD strings from start_date to end_date inclusive."""
    start = datetime.fromisoformat(start_date_str).date()
    end = datetime.fromisoformat(end_date_str).date()
    d = start
    while d <= end:
        yield d.isoformat()
        d += timedelta(days=1)


def extract_daily_stats_for_player(oauth, player_key, stats_date):
    """
    Call player/{player_key}/stats;date={stats_date}
    and return a list of dicts:
      { player_key, player_name, coverage, period, timestamp, stat_id, stat_value }
    """
    rel = f"player/{player_key}/stats;date={stats_date}"
    data = get_json(oauth, rel)

    fc = data.get("fantasy_content", {})
    player_node = fc.get("player")

    if player_node is None:
        return []

    # Player name
    name_obj = find_first(player_node, "name")
    if isinstance(name_obj, dict) and "full" in name_obj:
        player_name = name_obj["full"]
    else:
        player_name = find_first(name_obj, "full") if isinstance(name_obj, dict) else None
    if not player_name:
        player_name = "Unknown"

    # Player stats node
    player_stats = find_first(player_node, "player_stats")
    if not isinstance(player_stats, dict):
        return []

    coverage_type = player_stats.get("coverage_type") or find_first(player_stats, "coverage_type")
    period = (
        player_stats.get("date")
        or player_stats.get("season")
        or player_stats.get("week")
        or find_first(player_stats, "date")
        or find_first(player_stats, "season")
        or find_first(player_stats, "week")
    )

    stats_node = player_stats.get("stats") or find_first(player_stats, "stats")
    if stats_node is None:
        return []

    # stats_node is usually {"stat": [ {...}, {...} ]}
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
        # Sometimes wrapped in {"stat": {...}}
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
                "period": period or stats_date,
                "timestamp": stats_date,  # standardized daily timestamp
                "stat_id": str(stat_id),
                "stat_value": value,
            }
        )

    return rows


def build_full_parquet_from_daily():
    """Read all daily CSVs and write a single Parquet file with all stats."""
    if not os.path.isdir(DAILY_DIR):
        print(f"No {DAILY_DIR} directory found; skipping full Parquet build.")
        return None

    files = sorted(
        f for f in os.listdir(DAILY_DIR)
        if f.endswith(".csv")
    )
    if not files:
        print(f"No daily CSV files in {DAILY_DIR}; skipping full Parquet build.")
        return None

    dfs = []
    for fname in files:
        path = os.path.join(DAILY_DIR, fname)
        try:
            df = pd.read_csv(path, dtype=str)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"Failed to read {path}: {type(e).__name__} {e}")

    if not dfs:
        print("All daily CSVs were empty or unreadable; skipping Parquet.")
        return None

    full_df = pd.concat(dfs, ignore_index=True)
    # Optional: sort for nice ordering
    full_df.sort_values(by=["timestamp", "player_key", "stat_id"], inplace=True, ignore_index=True)

    full_df.to_parquet(FULL_PARQUET, index=False)
    print(f"Saved {len(full_df)} total rows to {FULL_PARQUET}")
    return full_df


def build_combined_parquet(base_for_combined, full_stats_df):
    """
    Join rosters/league_players with full stats and write combined_player_view_full.parquet.
    """
    if full_stats_df is None or full_stats_df.empty:
        print("No full stats DataFrame provided; skipping combined Parquet.")
        return

    base = base_for_combined.copy()
    for col in ["player_key", "player_name"]:
        if col not in base.columns:
            base[col] = base_for_combined.get(col)

    # Prefer join on both key + name if available
    if "player_name" in base.columns and "player_name" in full_stats_df.columns:
        merged = base.merge(
            full_stats_df,
            on=["player_key", "player_name"],
            how="left",
            validate="m:m",
        )
    else:
        merged = base.merge(
            full_stats_df,
            on=["player_key"],
            how="left",
            validate="m:m",
        )

    merged.to_parquet(COMBINED_PARQUET, index=False)
    print(f"Saved {len(merged)} rows to {COMBINED_PARQUET}")


def main():
    # 0) Basic checks
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"{CONFIG_FILE} not found")

    # Ensure daily directory exists
    os.makedirs(DAILY_DIR, exist_ok=True)

    # 1) OAuth
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid inside GitHub Actions – refresh locally first.")

    # 2) Make sure league_players.csv exists
    if not os.path.exists("league_players.csv"):
        raise SystemExit("league_players.csv not found. Run fetch_players_and_stats.py first.")

    league_players = pd.read_csv("league_players.csv", dtype=str)
    if "player_key" not in league_players.columns:
        raise SystemExit("league_players.csv missing 'player_key' column.")

    # 3) If we have team_rosters.csv, use that as base for combined view
    base_for_combined = league_players.copy()
    if os.path.exists("team_rosters.csv"):
        try:
            rosters = pd.read_csv("team_rosters.csv", dtype=str)
            if "player_key" in rosters.columns:
                base_for_combined = rosters.copy()
                print("Using team_rosters.csv as base for combined view.")
        except Exception as e:
            print("Failed to read team_rosters.csv, falling back to league_players:", type(e).__name__, e)

    # 4) Determine the full date range for the league
    league_start = get_league_start_date(oauth)
    league_current = get_league_current_date(oauth)

    print(f"League date range: {league_start} to {league_current}")

    # 5) Get list of unique players
    player_keys = sorted(league_players["player_key"].dropna().unique().tolist())
    print(f"Found {len(player_keys)} unique player_keys in league_players.csv")

    # 6) Fetch missing daily files
    for stats_date in date_range_iso(league_start, league_current):
        daily_path = os.path.join(DAILY_DIR, f"{stats_date}.csv")

        if os.path.exists(daily_path):
            print(f"Daily file for {stats_date} already exists ({daily_path}), skipping API calls for this date.")
            continue

        print(f"\n=== Fetching DAILY stats for date: {stats_date} ===")
        date_rows = []

        for idx, pk in enumerate(player_keys, start=1):
            print(f"[{stats_date}] [{idx}/{len(player_keys)}] Fetching stats for player {pk}")
            try:
                rows = extract_daily_stats_for_player(oauth, pk, stats_date)
            except Exception as e:
                print(f"Failed to fetch stats for {pk} on {stats_date}: {type(e).__name__} {e}")
                rows = []
            date_rows.extend(rows)

        # Build daily DataFrame and save
        if date_rows:
            df_date = pd.DataFrame(date_rows)
        else:
            # Create an empty file with correct columns to signal "processed"
            df_date = pd.DataFrame(
                columns=["player_key", "player_name", "coverage", "period", "timestamp", "stat_id", "stat_value"]
            )
            print(f"WARNING: No stats found for any player on {stats_date}")

        df_date.to_csv(daily_path, index=False)
        print(f"Saved {len(df_date)} rows to {daily_path}")

    # 7) Build full Parquet from daily CSVs
    full_stats_df = build_full_parquet_from_daily()

    # 8) Build combined view Parquet
    if full_stats_df is not None:
        build_combined_parquet(base_for_combined, full_stats_df)


if __name__ == "__main__":
    main()
