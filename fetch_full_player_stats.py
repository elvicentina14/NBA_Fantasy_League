# fetch_full_player_stats.py
#
# Behavior:
# - Each run:
#     * Uses TODAY'S UTC DATE as the snapshot label (YYYY-MM-DD).
#     * For every player, calls Yahoo:
#           player/{player_key}/stats;date={snapshot_date}
#       This behaves like "season totals as of now".
#     * Writes one CSV per day: player_stats_daily/YYYY-MM-DD.csv
# - Then builds player_stats_full.parquet with:
#     * stat_value_num = season total as of that snapshot date
#     * daily_value    = increment vs previous snapshot (per player_key + stat_id)
# - Then builds combined_player_view_full.parquet by joining stats with:
#     * team_rosters.csv if it exists
#     * else league_players.csv
#
# Files:
#   - league_players.csv      (built from league/{LEAGUE_KEY}/players if missing)
#   - team_rosters.csv        (from fetch_rosters_and_standings.py)
#   - player_stats_daily/*.csv
#   - player_stats_full.parquet
#   - combined_player_view_full.parquet

from __future__ import annotations

from yahoo_oauth import OAuth2
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"

# 2025–2026 season start date for Yahoo NBA fantasy
SEASON_START = "2025-10-21"


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
    If non-200 or JSON error, log and return {} so that player is skipped.
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
        print(f"JSON decode error for URL {url} (status {resp.status_code})")
        return {}


def get_snapshot_date(oauth: Optional[OAuth2] = None) -> str:
    """
    Decide which date label to use for this snapshot.

    We IGNORE league.current_date for the label and just use today's UTC date.
    """
    today_str = datetime.now(timezone.utc).date().isoformat()
    print(f"Using today's UTC date for snapshot: {today_str}")

    # Log Yahoo's current_date for debugging only (not used as the label)
    if oauth is not None and LEAGUE_KEY:
        try:
            data = get_json(oauth, f"league/{LEAGUE_KEY}")
            league_current = find_first(data, "current_date")
            print(f"(Yahoo league.current_date = {league_current})")
        except Exception as e:
            print("Could not read league.current_date (ignored):", type(e).__name__, e)

    return today_str


# ---------- League players builder ----------

def _collect_player_nodes(obj: Any, out: List[Dict[str, Any]]) -> None:
    """Recursively collect dicts that look like player records (have player_key)."""
    if isinstance(obj, dict):
        if "player_key" in obj:
            out.append(obj)
        for v in obj.values():
            _collect_player_nodes(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_player_nodes(item, out)


def _extract_player_name(node: Dict[str, Any]) -> str:
    """Best-effort extraction of full player name."""
    name_obj = node.get("name") or find_first(node, "name")
    full_name = None

    if isinstance(name_obj, dict) and "full" in name_obj:
        full_name = name_obj["full"]
    elif isinstance(name_obj, dict):
        full_name = find_first(name_obj, "full")

    if not full_name:
        full_name = find_first(node, "full")

    if not full_name:
        full_name = "Unknown"

    return str(full_name)


def build_league_players(oauth: OAuth2) -> pd.DataFrame:
    """
    Build league_players.csv from:
       league/{LEAGUE_KEY}/players;start=N;count=25
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    print("Building league_players.csv from league players endpoint...")
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

            full_name = _extract_player_name(node)

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


# ---------- Yahoo stats call ----------

def extract_cumulative_stats_for_player(
    oauth: OAuth2,
    player_key: str,
    snapshot_date: str,
) -> List[Dict[str, Any]]:
    """
    Call:
      player/{player_key}/stats;date={snapshot_date}

    For your league, this behaves like "season totals as of now".
    We label that with snapshot_date and later diff snapshots to get daily_value.
    """
    rel = f"player/{player_key}/stats;date={snapshot_date}"
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
        or snapshot_date
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
                "timestamp": snapshot_date,   # our snapshot label
                "stat_id": str(stat_id),
                "stat_value": value,          # cumulative season total at this snapshot
            }
        )

    return rows


# ---------- parquet builders ----------

def build_full_parquet_from_daily() -> Optional[pd.DataFrame]:
    """
    Combine all daily CSVs and compute:
      - stat_value_num: cumulative total (season-to-date)
      - daily_value: per-snapshot increment vs previous snapshot for that player+stat_id
    Only keeps rows timestamp >= SEASON_START (2025-10-21).
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

    # Filter for current season only
    if "timestamp" in full_df.columns:
        full_df = full_df[full_df["timestamp"] >= SEASON_START].copy()

    # Core types
    for col in ["player_key", "stat_id", "timestamp"]:
        if col in full_df.columns:
            full_df[col] = full_df[col].astype(str)

    # Numeric cumulative
    full_df["stat_value_num"] = pd.to_numeric(full_df.get("stat_value"), errors="coerce")

    # Sort for diff
    full_df.sort_values(
        by=["player_key", "stat_id", "timestamp"],
        inplace=True,
        ignore_index=True,
    )

    # daily_value = cumulative - previous cumulative per player+stat
    full_df["daily_value"] = full_df.groupby(
        ["player_key", "stat_id"]
    )["stat_value_num"].diff()

    # First snapshot per player+stat: use that snapshot's cumulative as daily_value
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


def build_combined_parquet(
    base_for_combined: pd.DataFrame,
    full_stats_df: pd.DataFrame,
) -> None:
    """
    Join rosters/league_players with full stats and write combined_player_view_full.parquet.

    - If team_rosters.csv exists, base includes team_key, team_name, position.
    - We ALWAYS join on player_key ONLY (names can differ).
    - We drop base player_name so we only keep the one from stats.
    """
    if full_stats_df is None or full_stats_df.empty:
        print("No full stats DataFrame provided; skipping combined Parquet.")
        return

    base = base_for_combined.copy()

    if "player_key" not in base.columns:
        raise SystemExit("Base DataFrame for combined view is missing 'player_key'.")

    # Remove base player_name so we keep a single player_name from stats
    if "player_name" in base.columns:
        base = base.drop(columns=["player_name"])

    merged = base.merge(
        full_stats_df,
        on="player_key",
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

    # league_players.csv = master player list
    rebuild_lp = False
    if not os.path.exists("league_players.csv"):
        print("league_players.csv not found – will build from API.")
        rebuild_lp = True
    else:
        print("Using existing league_players.csv")
        lp_tmp = pd.read_csv("league_players.csv", dtype=str)
        if "player_name" in lp_tmp.columns and len(lp_tmp) > 0:
            frac_unknown = (lp_tmp["player_name"] == "Unknown").mean()
            print(f"Existing league_players.csv: {frac_unknown:.1%} of names are 'Unknown'.")
            if frac_unknown > 0.9:
                print("Too many 'Unknown' names – rebuilding league_players from API.")
                rebuild_lp = True
        else:
            rebuild_lp = True

    if rebuild_lp:
        league_players = build_league_players(oauth)
        if league_players.empty:
            raise SystemExit("Could not build league_players.csv from API; aborting.")
        league_players.to_csv("league_players.csv", index=False)
        print("Saved rebuilt league_players.csv")
    else:
        league_players = pd.read_csv("league_players.csv", dtype=str)

    if "player_key" not in league_players.columns:
        raise SystemExit("league_players.csv missing 'player_key' column.")

    # Base table for combined view:
    #   Prefer team_rosters.csv (team_key, team_name, etc.) if it exists,
    #   otherwise fallback to league_players.
    base_for_combined = league_players.copy()
    if os.path.exists("team_rosters.csv"):
        try:
            rosters = pd.read_csv("team_rosters.csv", dtype=str)
            if "player_key" in rosters.columns:
                base_for_combined = rosters.copy()
                print("Using team_rosters.csv as base for combined view.")
            else:
                print("team_rosters.csv missing 'player_key'; falling back to league_players.")
        except Exception as e:
            print("Failed to read team_rosters.csv, falling back to league_players:", type(e).__name__, e)

    # Snapshot date: TODAY (UTC)
    snapshot_date = get_snapshot_date(oauth)
    print(f"Snapshotting cumulative stats for date label: {snapshot_date}")

    # Build today's daily CSV (overwrites previous same-date file)
    player_keys = sorted(league_players["player_key"].dropna().unique().tolist())
    print(f"Found {len(player_keys)} player_keys.")

    daily_rows: List[Dict[str, Any]] = []
    for idx, pk in enumerate(player_keys, start=1):
        print(f"[{snapshot_date}] [{idx}/{len(player_keys)}] Fetching stats for player {pk}")
        try:
            rows = extract_cumulative_stats_for_player(oauth, pk, snapshot_date)
        except Exception as e:
            print(f"Failed to fetch stats for {pk} on {snapshot_date}: {type(e).__name__} {e}")
            rows = []
        daily_rows.extend(rows)

    daily_path = os.path.join(DAILY_DIR, f"{snapshot_date}.csv")

    if daily_rows:
        df_day = pd.DataFrame(daily_rows)
    else:
        df_day = pd.DataFrame(
            columns=["player_key", "player_name", "coverage", "period", "timestamp", "stat_id", "stat_value"]
        )
        print(f"WARNING: No stats found for any player on {snapshot_date}")

    df_day.to_csv(daily_path, index=False)
    print(f"Saved {len(df_day)} rows to {daily_path}")

    # Rebuild parquet from ALL daily CSVs (today + future snapshots)
    full_stats_df = build_full_parquet_from_daily()
    if full_stats_df is not None:
        build_combined_parquet(base_for_combined, full_stats_df)


if __name__ == "__main__":
    main()
