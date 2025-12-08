# fetch_full_player_stats.py
#
# Single-script pipeline:
# - Fetch league player list from Yahoo -> league_players.csv
# - Fetch season-to-date totals AS OF league.current_date (or today's UTC) for every player
# - Write/overwrite player_stats_daily/YYYY-MM-DD.csv for that scoring date
# - Rebuild:
#       player_stats_full.parquet  (cumulative + daily_value)
#       combined_player_view_full.parquet (league_players joined to stats)
#
# From today onward:
#   - First day: daily_value = season total that day
#   - Later days: daily_value = today's total - previous day's total (per player+stat_id)

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

def ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj: Any, key: str) -> Any:
    """Recursively find the first occurrence of key anywhere in nested dict/list."""
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


def get_json(oauth: OAuth2, relative_path: str) -> Dict[str, Any]:
    """
    Call Yahoo Fantasy API with ?format=json appended.
    Be tolerant of non-JSON / 999 responses – return {} instead of crashing.
    """
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + relative_path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}format=json"

    resp = oauth.session.get(url)
    if resp.status_code != 200:
        print(f"Non-200 response for URL {url}: {resp.status_code}")
        return {}

    try:
        return resp.json()
    except ValueError:
        print(f"JSON decode error for URL: {url} (status {resp.status_code})")
        return {}


def get_league_current_date(oauth: OAuth2) -> str:
    """
    Use league.current_date (fantasy scoring date) when Yahoo returns it.
    Fallback: today's UTC date.
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    try:
        data = get_json(oauth, f"league/{LEAGUE_KEY}")
        current_date = find_first(data, "current_date")
        if isinstance(current_date, str) and len(current_date) == 10:
            print(f"(Yahoo) league.current_date = {current_date}")
            return current_date
        else:
            print("(Yahoo) league.current_date missing or invalid in response.")
    except Exception as e:
        print("Could not get league current_date from API:", type(e).__name__, e)

    today_str = datetime.now(timezone.utc).date().isoformat()
    print(f"Using today's UTC date for snapshot: {today_str}")
    return today_str


# ---------- league players ----------

def get_all_league_players(oauth: OAuth2) -> pd.DataFrame:
    """
    Fetch the full list of players tied to this league using league/{LEAGUE_KEY}/players pagination.
    Writes league_players.csv and returns it as a DataFrame.
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    rows: List[Dict[str, Any]] = []
    start = 0
    page = 0

    while True:
        rel = f"league/{LEAGUE_KEY}/players;start={start}"
        data = get_json(oauth, rel)
        fc = data.get("fantasy_content", {})
        league = fc.get("league")
        if not league:
            print("No 'league' key in league/players response; stopping.")
            break

        league_list = ensure_list(league)
        if len(league_list) < 2:
            print("League list too short in league/players response; stopping.")
            break

        # Typical Yahoo shape: league_list[1]['players'] -> {'count': N, '0': {...}, ...}
        players_block = league_list[1].get("players")
        if not isinstance(players_block, dict):
            print("No 'players' dict in league/players response; stopping.")
            break

        count_str = players_block.get("count", "0")
        try:
            count = int(count_str)
        except (TypeError, ValueError):
            count = 0

        if count == 0:
            print("players_block count=0; stopping pagination.")
            break

        print(f"league/players page {page}, start={start}, count={count}")

        for i in range(count):
            entry = players_block.get(str(i))
            if not entry:
                continue
            player_node = entry.get("player")
            if not player_node:
                continue

            # player_node is usually a list; use generic find_first helpers
            player_key = find_first(player_node, "player_key")
            name_obj = find_first(player_node, "name")
            if isinstance(name_obj, dict) and "full" in name_obj:
                full_name = name_obj["full"]
            else:
                full_name = find_first(name_obj, "full") if isinstance(name_obj, dict) else None

            editorial_team_abbr = find_first(player_node, "editorial_team_abbr")
            display_position = find_first(player_node, "display_position")

            if not player_key:
                continue

            rows.append(
                {
                    "player_key": str(player_key),
                    "player_name": full_name or "",
                    "team": editorial_team_abbr or "",
                    "position": display_position or "",
                }
            )

        start += count
        page += 1

    if not rows:
        raise SystemExit("Failed to fetch any players for this league from league/players.")

    df = pd.DataFrame(rows).drop_duplicates(subset=["player_key"]).reset_index(drop=True)
    df.to_csv("league_players.csv", index=False)
    print(f"Wrote league_players.csv rows: {len(df)}")

    return df


# ---------- Yahoo stats call (season-to-date snapshot for a date) ----------

def extract_cumulative_stats_for_player(oauth: OAuth2, player_key: str, stats_date: str) -> List[Dict[str, Any]]:
    """
    Call player/{player_key}/stats;type=date;date={stats_date}.
    Yahoo returns season-to-date totals AS OF that scoring date.
    """
    rel = f"player/{player_key}/stats;type=date;date={stats_date}"
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

    if isinstance(stats_node, dict):
        if "stat" in stats_node:
            stat_items = ensure_list(stats_node["stat"])
        else:
            stat_items = [stats_node]
    elif isinstance(stats_node, list):
        stat_items = stats_node
    else:
        stat_items = []

    rows: List[Dict[str, Any]] = []
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
                "coverage": coverage_type or "season",
                "period": period,
                "timestamp": stats_date,   # label = scoring date
                "stat_id": str(stat_id),
                "stat_value": value,       # cumulative as of this date
            }
        )

    return rows


# ---------- parquet builders ----------

def build_full_parquet_from_daily() -> pd.DataFrame | None:
    """
    Combine all daily CSVs and compute:
      - stat_value_num: cumulative total (season-to-date)
      - daily_value: per-date increment vs previous day for that player+stat_id
    """
    if not os.path.isdir(DAILY_DIR):
        print(f"No {DAILY_DIR} directory found; skipping full Parquet build.")
        return None

    files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".csv"))
    if not files:
        print(f"No daily CSV files in {DAILY_DIR}; skipping full Parquet build.")
        return None

    dfs: List[pd.DataFrame] = []
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

    # Clean core types
    for col in ["player_key", "stat_id", "timestamp"]:
        if col in full_df.columns:
            full_df[col] = full_df[col].astype(str)

    # Numeric cumulative value
    full_df["stat_value_num"] = pd.to_numeric(full_df.get("stat_value"), errors="coerce")

    # Sort for diff calculation
    full_df.sort_values(
        by=["player_key", "stat_id", "timestamp"],
        inplace=True,
        ignore_index=True,
    )

    # daily_value = cumulative - previous cumulative per player+stat
    full_df["daily_value"] = full_df.groupby(
        ["player_key", "stat_id"]
    )["stat_value_num"].diff()

    # First date per player+stat: use that day's cumulative as daily_value
    full_df["daily_value"] = full_df["daily_value"].fillna(full_df["stat_value_num"])

    # Save Parquet
    full_df.to_parquet(FULL_PARQUET, index=False)

    ts = full_df["timestamp"].dropna().astype(str)
    if not ts.empty:
        print(
            f"Saved {len(full_df)} rows to {FULL_PARQUET}. "
            f"Dates: {ts.min()} → {ts.max()} ({ts.nunique()} distinct days)"
        )
    else:
        print(f"Saved {len(full_df)} rows to {FULL_PARQUET}, timestamp column empty.")

    return full_df


def build_combined_parquet(league_players: pd.DataFrame,
                           full_stats_df: pd.DataFrame) -> None:
    """
    Join league_players with full stats and write combined_player_view_full.parquet.
    """
    if full_stats_df is None or full_stats_df.empty:
        print("No full stats DataFrame provided; skipping combined Parquet.")
        return

    base = league_players.copy()
    for col in ["player_key", "player_name"]:
        if col not in base.columns:
            base[col] = base.get(col, "")

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

    if "timestamp" in merged.columns:
        ts = merged["timestamp"].dropna().astype(str)
        if not ts.empty:
            print(
                f"Saved {len(merged)} rows to {COMBINED_PARQUET}. "
                f"Dates: {ts.min()} → {ts.max()} ({ts.nunique()} distinct days)"
            )
        else:
            print(f"Saved {len(merged)} rows to {COMBINED_PARQUET}, timestamp empty.")
    else:
        print(f"Saved {len(merged)} rows to {COMBINED_PARQUET}, no timestamp column present.")


# ---------- main ----------

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"{CONFIG_FILE} not found")

    os.makedirs(DAILY_DIR, exist_ok=True)

    # OAuth
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid inside GitHub Actions – refresh locally first.")

    # Always refresh league_players from Yahoo each run
    league_players = get_all_league_players(oauth)

    # Determine fantasy scoring date
    stats_date = get_league_current_date(oauth)
    print(f"Snapshotting cumulative stats for date label: {stats_date}")

    # Fetch season-to-date totals for all league players
    player_keys = sorted(league_players["player_key"].dropna().unique().tolist())
    print(f"Found {len(player_keys)} player_keys.")

    daily_rows: List[Dict[str, Any]] = []
    for idx, pk in enumerate(player_keys, start=1):
        print(f"[{stats_date}] [{idx}/{len(player_keys)}] Fetching stats for player {pk}")
        try:
            rows = extract_cumulative_stats_for_player(oauth, pk, stats_date)
        except Exception as e:
            print(f"Failed to fetch stats for {pk} on {stats_date}: {type(e).__name__} {e}")
            rows = []
        daily_rows.extend(rows)

    daily_path = os.path.join(DAILY_DIR, f"{stats_date}.csv")

    if daily_rows:
        df_day = pd.DataFrame(daily_rows)
    else:
        df_day = pd.DataFrame(
            columns=["player_key", "player_name", "coverage", "period", "timestamp", "stat_id", "stat_value"]
        )
        print(f"WARNING: No stats found for any player on {stats_date}")

    df_day.to_csv(daily_path, index=False)
    print(f"Saved {len(df_day)} rows to {daily_path}")

    # Rebuild parquet from ALL daily CSVs (today + any future ones)
    full_stats_df = build_full_parquet_from_daily()
    if full_stats_df is not None:
        build_combined_parquet(league_players, full_stats_df)


if __name__ == "__main__":
    main()
