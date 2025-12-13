#!/usr/bin/env python3
"""
fetch_full_player_stats.py

- Ensures league_players.csv exists (builds it from team_rosters.csv or Yahoo).
- Snapshots season-to-date cumulative stats for TODAY (UTC) for every player_key.
- Writes:
  - player_stats_daily/YYYY-MM-DD.csv
  - player_stats_full.parquet
  - combined_player_view_full.parquet  (base = team_rosters.csv if exists, else league_players.csv)
"""
from yahoo_oauth import OAuth2
import os
from datetime import datetime, timezone
from typing import Any, Dict, List
import pandas as pd

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

DAILY_DIR = "player_stats_daily"
FULL_PARQUET = "player_stats_full.parquet"
COMBINED_PARQUET = "combined_player_view_full.parquet"

SEASON_START = "2025-10-01"  # adjust if you want a different season cutoff

# ------- helpers ------- #

def ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def find_first(obj: Any, key: str):
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
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + relative_path
    if "format=json" not in url:
        url += ("&" if "?" in url else "?") + "format=json"
    resp = oauth.session.get(url)
    try:
        resp.raise_for_status()
    except Exception:
        print(f"Non-200 response for {url}: {getattr(resp,'status_code',None)}")
        return {}
    try:
        return resp.json()
    except Exception:
        print(f"JSON decode error for {url}")
        return {}

def get_today_utc_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()

# ------- build league players ------- #

def build_league_players_from_rosters() -> pd.DataFrame:
    if not os.path.exists("team_rosters.csv"):
        return pd.DataFrame(columns=["player_key","player_name"])
    df = pd.read_csv("team_rosters.csv", dtype=str)
    if "player_key" in df.columns:
        dfp = df[["player_key","player_name"]].drop_duplicates().reset_index(drop=True)
        return dfp
    return pd.DataFrame(columns=["player_key","player_name"])

def fetch_league_players_from_api(oauth: OAuth2) -> pd.DataFrame:
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")
    players = {}
    start = 0
    page_size = 25
    while True:
        rel = f"league/{LEAGUE_KEY}/players;start={start};count={page_size}"
        data = get_json(oauth, rel)
        if not data:
            break
        # find all player nodes
        player_nodes = []
        def collect_players(obj):
            if isinstance(obj, dict):
                if "player_key" in obj:
                    player_nodes.append(obj)
                for v in obj.values():
                    collect_players(v)
            elif isinstance(obj, list):
                for i in obj:
                    collect_players(i)
        collect_players(data)
        if not player_nodes:
            break
        added = 0
        for node in player_nodes:
            pk = str(node.get("player_key","")).strip()
            if not pk:
                continue
            name_node = node.get("name") or find_first(node, "name")
            full = None
            if isinstance(name_node, dict):
                full = name_node.get("full") or find_first(name_node, "full")
            elif isinstance(name_node, str):
                full = name_node
            if not full:
                full = find_first(node, "full") or "Unknown"
            if pk not in players:
                players[pk] = {"player_key": pk, "player_name": full}
                added += 1
        if added == 0:
            break
        start += page_size
    if not players:
        return pd.DataFrame(columns=["player_key","player_name"])
    df = pd.DataFrame(list(players.values()))
    df.sort_values("player_key", inplace=True, ignore_index=True)
    return df

# ------- stats extraction ------- #

def extract_cumulative_stats_for_player(oauth: OAuth2, player_key: str, stats_date: str) -> List[Dict[str,Any]]:
    rel = f"player/{player_key}/stats;type=date;date={stats_date}"
    data = get_json(oauth, rel)
    fc = data.get("fantasy_content", {})
    player_node = fc.get("player")
    if player_node is None:
        # sometimes fantasy_content is a list or nested differently - search for player node
        player_node = find_first(data, "player")
        if player_node is None:
            return []
    # name
    name_obj = find_first(player_node, "name")
    player_name = None
    if isinstance(name_obj, dict):
        player_name = name_obj.get("full") or find_first(name_obj, "full")
    elif isinstance(name_obj, str):
        player_name = name_obj
    if not player_name:
        player_name = find_first(player_node, "full") or "Unknown"

    player_stats = find_first(player_node, "player_stats")
    if not isinstance(player_stats, dict):
        # some shapes have stats directly under player
        player_stats = find_first(player_node, "stats") or player_stats
    if not player_stats or not isinstance(player_stats, dict):
        return []

    coverage_type = player_stats.get("coverage_type") or find_first(player_stats, "coverage_type") or "season"
    period = player_stats.get("date") or player_stats.get("season") or player_stats.get("week") or stats_date

    stats_node = player_stats.get("stats") or find_first(player_stats, "stats")
    if stats_node is None:
        return []

    # `stats_node` can be { "stat": [ ... ] } or a list etc.
    stat_items = []
    if isinstance(stats_node, dict):
        if "stat" in stats_node:
            stat_items = ensure_list(stats_node["stat"])
        else:
            stat_items = [stats_node]
    elif isinstance(stats_node, list):
        stat_items = stats_node
    else:
        stat_items = []

    rows = []
    for item in stat_items:
        # item can be {"stat": {...}} or {...} directly
        s = item.get("stat") if isinstance(item, dict) and "stat" in item else item
        stat_id = find_first(s, "stat_id")
        value = find_first(s, "value")
        if stat_id is None:
            continue
        rows.append({
            "player_key": str(player_key),
            "player_name": str(player_name),
            "coverage": coverage_type,
            "period": period,
            "timestamp": stats_date,
            "stat_id": str(stat_id),
            "stat_value": str(value) if value is not None else None
        })
    return rows

# ------- parquet builders ------- #

def build_full_parquet_from_daily() -> pd.DataFrame | None:
    if not os.path.isdir(DAILY_DIR):
        print("No daily dir.")
        return None
    files = sorted(f for f in os.listdir(DAILY_DIR) if f.endswith(".csv"))
    if not files:
        print("No daily CSVs")
        return None
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(os.path.join(DAILY_DIR, f), dtype=str)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print("Failed reading", f, e)
    if not dfs:
        return None
    full_df = pd.concat(dfs, ignore_index=True)
    for col in ["player_key","player_name","stat_id","timestamp"]:
        if col in full_df.columns:
            full_df[col] = full_df[col].astype(str)
    # limit to season start
    if "timestamp" in full_df.columns:
        full_df = full_df[full_df["timestamp"] >= SEASON_START].copy()
    full_df["stat_value_num"] = pd.to_numeric(full_df.get("stat_value"), errors="coerce")
    full_df.sort_values(by=["player_key","stat_id","timestamp"], inplace=True, ignore_index=True)
    full_df["daily_value"] = full_df.groupby(["player_key","stat_id"])["stat_value_num"].diff()
    full_df["daily_value"] = full_df["daily_value"].fillna(full_df["stat_value_num"])
    full_df.to_parquet(FULL_PARQUET, index=False)
    ts = full_df["timestamp"].dropna().astype(str)
    if not ts.empty:
        print(f"Saved {len(full_df)} rows to {FULL_PARQUET}. Dates: {ts.min()} → {ts.max()} ({ts.nunique()} distinct days)")
    else:
        print(f"Saved {len(full_df)} rows to {FULL_PARQUET}, timestamp empty.")
    return full_df

def build_combined_parquet(base_for_combined: pd.DataFrame, full_stats_df: pd.DataFrame):
    if full_stats_df is None or full_stats_df.empty:
        print("No full stats to build combined.")
        return
    if "player_key" not in base_for_combined.columns:
        raise SystemExit("Base missing player_key")
    # drop player's name from base to prefer stats name as canonical
    base = base_for_combined.copy()
    if "player_name" in base.columns:
        base = base.drop(columns=["player_name"])
    merged = base.merge(full_stats_df, on="player_key", how="left", validate="m:m")
    merged.to_parquet(COMBINED_PARQUET, index=False)
    if "timestamp" in merged.columns:
        ts = merged["timestamp"].dropna().astype(str)
        if not ts.empty:
            print(f"Saved {len(merged)} rows to {COMBINED_PARQUET}. Dates: {ts.min()} → {ts.max()} ({ts.nunique()} distinct days)")
        else:
            print(f"Saved {len(merged)} rows to {COMBINED_PARQUET}, timestamp empty.")
    else:
        print(f"Saved {len(merged)} rows to {COMBINED_PARQUET}, no timestamp present.")

# ------- main ------- #

def main():
    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"{CONFIG_FILE} not found")
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid in GH Actions - refresh locally.")

    os.makedirs(DAILY_DIR, exist_ok=True)

    # base for combined view: prefer team_rosters.csv
    if os.path.exists("team_rosters.csv"):
        try:
            base_df = pd.read_csv("team_rosters.csv", dtype=str)
            print("Using team_rosters.csv as base for combined view.")
        except Exception as e:
            print("Failed to read team_rosters.csv:", e)
            base_df = pd.DataFrame(columns=["player_key","player_name"])
    else:
        # if no rosters, build league_players.csv first or use API
        if os.path.exists("league_players.csv"):
            base_df = pd.read_csv("league_players.csv", dtype=str)
        else:
            lp = fetch_league_players_from_api(oauth)
            if lp.empty:
                base_df = pd.DataFrame(columns=["player_key","player_name"])
            else:
                lp.to_csv("league_players.csv", index=False)
                base_df = lp

    # ensure we have league players list for iterating
    if os.path.exists("league_players.csv"):
        lp_df = pd.read_csv("league_players.csv", dtype=str)
    else:
        # derive from base_df (rosters) if present
        if "player_key" in base_df.columns:
            lp_df = base_df[["player_key","player_name"]].drop_duplicates().reset_index(drop=True)
            lp_df.to_csv("league_players.csv", index=False)
        else:
            lp_df = pd.DataFrame(columns=["player_key","player_name"])

    if "player_key" not in lp_df.columns:
        raise SystemExit("league_players.csv missing player_key column.")

    stats_date = get_today_utc_str()
    print("Snapshot date (UTC):", stats_date)

    player_keys = sorted(lp_df["player_key"].dropna().unique().tolist())
    print("Found", len(player_keys), "players to fetch.")

    daily_rows = []
    for idx, pk in enumerate(player_keys, start=1):
        print(f"[{stats_date}] [{idx}/{len(player_keys)}] Fetching stats for {pk}")
        try:
            rows = extract_cumulative_stats_for_player(oauth, pk, stats_date)
        except Exception as e:
            print("Failed for", pk, e)
            rows = []
        daily_rows.extend(rows)

    daily_path = os.path.join(DAILY_DIR, f"{stats_date}.csv")
    if daily_rows:
        df_day = pd.DataFrame(daily_rows)
    else:
        df_day = pd.DataFrame(columns=["player_key","player_name","coverage","period","timestamp","stat_id","stat_value"])
        print(f"WARNING: No stats found for any player on {stats_date}")

    df_day.to_csv(daily_path, index=False)
    print(f"Saved {len(df_day)} rows to {daily_path}")

    full_stats_df = build_full_parquet_from_daily()
    if full_stats_df is not None:
        build_combined_parquet(base_df, full_stats_df)

if __name__ == "__main__":
    main()
