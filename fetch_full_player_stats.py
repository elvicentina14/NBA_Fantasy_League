# fetch_full_player_stats.py
#
# What this does
# --------------
# 1. Ensures we have league_players.csv
#    - If missing, calls Yahoo "league/{LEAGUE_KEY}/players" pages
#      and builds it automatically.
# 2. Fetches season-to-date stats as of TODAY ONLY for every player
#    in league_players.csv (no historical backfill – Yahoo 999s that).
# 3. Writes:
#       player_stats_daily/YYYY-MM-DD.csv
#       player_stats_full.parquet
#       combined_player_view_full.parquet
#
# player_stats_full.parquet columns:
#   player_key, player_name, coverage, period, timestamp,
#   stat_id, stat_value, stat_value_num, daily_value
#
# combined_player_view_full.parquet:
#   league_players joined to full stats (one row per player+stat+date)
#   JOIN IS ON player_key ONLY (to avoid name mismatch issues).

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from yahoo_oauth import OAuth2

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"

# *** IMPORTANT: 2025–2026 SEASON START DATE (Yahoo NBA fantasy) ***
SEASON_START = "2025-10-21"


# ================== Generic helpers ================== #

def ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj: Any, key: str) -> Any:
    """Recursively find the first occurrence of key anywhere in nested dict / list."""
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

    Returns {} instead of throwing if Yahoo returns 999 or non-JSON.
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


def get_today_utc_str() -> str:
    """YYYY-MM-DD in UTC (used when league.current_date is unavailable)."""
    return datetime.now(timezone.utc).date().isoformat()


# ================== League players builder ================== #

def _collect_player_nodes(obj: Any, out: List[Dict[str, Any]]) -> None:
    """
    Recursively walk the Yahoo JSON and collect dicts that look like player records.
    We only really need player_key + name, so we keep this generic.
    """
    if isinstance(obj, dict):
        if "player_key" in obj:
            out.append(obj)
        for v in obj.values():
            _collect_player_nodes(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_player_nodes(item, out)


def _extract_player_name_from_node(node: Dict[str, Any]) -> str:
    """
    Best-effort extraction of a player's full name from a Yahoo player node.
    Much more aggressive than before, to avoid 'Unknown' everywhere.
    """
    # Try direct "name" object
    name_obj = node.get("name") or find_first(node, "name")

    full_name = None
    if isinstance(name_obj, dict) and "full" in name_obj:
        full_name = name_obj["full"]
    elif isinstance(name_obj, dict):
        # As a fallback, search this dict for 'full'
        full_name = find_first(name_obj, "full")

    # Ultimate fallback: search entire node for any "full" key
    if not full_name:
        full_name = find_first(node, "full")

    if not full_name:
        full_name = "Unknown"

    return str(full_name)


def fetch_league_players(oauth: OAuth2) -> pd.DataFrame:
    """
    Fetch all players in the league via paged calls to:
      league/{LEAGUE_KEY}/players;start=N;count=25
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    print("Building league_players.csv from Yahoo league players endpoint...")
    players: Dict[str, Dict[str, str]] = {}

    start = 0
    page_size = 25
    page = 0

    while True:
        rel = f"league/{LEAGUE_KEY}/players;start={start};count={page_size}"
        print(f"Fetching league players page {page} (start={start})...")
        data = get_json(oauth, rel)
        if not data:
            print("Empty or invalid response for league players page – stopping pagination.")
            break

        tmp: List[Dict[str, Any]] = []
        _collect_player_nodes(data, tmp)

        added_this_page = 0
        for node in tmp:
            pk = str(node.get("player_key", "")).strip()
            if not pk:
                continue

            full_name = _extract_player_name_from_node(node)

            if pk not in players:
                players[pk] = {"player_key": pk, "player_name": full_name}
                added_this_page += 1

        print(f"  Found {len(tmp)} raw 'player' dicts, added {added_this_page} new players.")

        if added_this_page == 0:
            break

        page += 1
        start += page_size

    if not players:
        print("WARNING: did not find any players in league players responses.")
        return pd.DataFrame(columns=["player_key", "player_name"])

    df = pd.DataFrame(list(players.values()))
    df.sort_values(by="player_key", inplace=True, ignore_index=True)
    print(f"Built league_players DataFrame with {len(df)} players.")
    return df


# ================== Stats fetch (today only) ================== #

def extract_cumulative_stats_for_player(
    oauth: OAuth2, player_key: str, stats_date: str
) -> List[Dict[str, Any]]:
    """
    Call:
      player/{player_key}/stats;type=date;date={stats_date}

    We get season-to-date totals as of that fantasy date.
    """
    rel = f"player/{player_key}/stats;type=date;date={stats_date}"
    data = get_json(oauth, rel)

    fc = data.get("fantasy_content", {})
    player_node = fc.get("player")
    if player_node is None:
        return []

    # Name from stats payload (usually reliable)
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
                "player_name": str(player_name),
                "coverage": coverage_type or "season",
                "period": period,
                "timestamp": stats_date,
                "stat_id": str(stat_id),
                "stat_value": value,
            }
        )

    return rows


# ================== Parquet builders ================== #

def build_full_parquet_from_daily() -> pd.DataFrame | None:
    """
    Build player_stats_full.parquet from ALL CSVs in player_stats_daily,
    but only for rows on or after SEASON_START.
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

    # Core string fields
    for col in ["player_key", "player_name", "stat_id", "timestamp"]:
        if col in full_df.columns:
            full_df[col] = full_df[col].astype(str)

    # Filter for CURRENT SEASON ONLY
    if "timestamp" in full_df.columns:
        full_df = full_df[full_df["timestamp"] >= SEASON_START].copy()

    # Numeric cumulative value
    full_df["stat_value_num"] = pd.to_numeric(full_df.get("stat_value"), errors="coerce")

    # Sort for diff
    full_df.sort_values(
        by=["player_key", "stat_id", "timestamp"],
        inplace=True,
        ignore_index=True,
    )

    # daily_value = today's cumulative minus previous cumulative for same player+stat
    full_df["daily_value"] = full_df.groupby(
        ["player_key", "stat_id"]
    )["stat_value_num"].diff()

    # First date per player+stat gets full cumulative as daily_value
    full_df["daily_value"] = full_df["daily_value"].fillna(full_df["stat_value_num"])

    full_df.to_parquet(FULL_PARQUET, index=False)

    if "timestamp" in full_df.columns:
        ts = full_df["timestamp"].dropna().astype(str)
        if not ts.empty:
            print(
                f"Saved {len(full_df)} rows to {FULL_PARQUET}. "
                f"Dates: {ts.min()} → {ts.max()} ({ts.nunique()} distinct days)"
            )
        else:
            print(f"Saved {len(full_df)} rows to {FULL_PARQUET}, timestamp column empty.")
    else:
        print(f"Saved {len(full_df)} rows to {FULL_PARQUET}, no timestamp column present.")

    return full_df


def build_combined_parquet(base_for_combined: pd.DataFrame,
                           full_stats_df: pd.DataFrame) -> None:
    """
    Join base_for_combined (from league_players.csv) with stats and write
    combined_player_view_full.parquet.

    *** IMPORTANT ***
    We ALWAYS join on player_key ONLY to avoid mismatch when names differ
    between league_players.csv and stats payloads.
    """
    if full_stats_df is None or full_stats_df.empty:
        print("No full stats DataFrame provided; skipping combined Parquet.")
        return

    base = base_for_combined.copy()

    # Ensure player_key exists
    if "player_key" not in base.columns:
        raise SystemExit("Base DataFrame for combined view is missing 'player_key'.")

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


# ================== main ================== #

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"{CONFIG_FILE} not found (must be oauth2.json)")

    os.makedirs(DAILY_DIR, exist_ok=True)

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid inside GitHub Actions – refresh locally first.")

    # Build or load league_players.csv
    if not os.path.exists("league_players.csv"):
        print("league_players.csv not found – trying to fetch league players from Yahoo...")
        lp_df = fetch_league_players(oauth)
        if lp_df.empty:
            raise SystemExit("Could not build league_players.csv from API; aborting.")
        lp_df.to_csv("league_players.csv", index=False)
        print("Saved league_players.csv")
    else:
        print("Using existing league_players.csv")
        lp_df = pd.read_csv("league_players.csv", dtype=str)

    if "player_key" not in lp_df.columns:
        raise SystemExit("league_players.csv missing 'player_key' column.")

    stats_date = get_today_utc_str()
    print(f"Snapshotting cumulative stats for date (UTC): {stats_date}")

    player_keys = sorted(lp_df["player_key"].dropna().unique().tolist())
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

    # Build Parquet over all days we’ve ever saved (current season only)
    full_stats_df = build_full_parquet_from_daily()
    if full_stats_df is not None:
        build_combined_parquet(lp_df, full_stats_df)


if __name__ == "__main__":
    main()
