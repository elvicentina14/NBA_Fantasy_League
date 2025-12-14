import os
import sys
import pandas as pd
from yahoo_oauth import OAuth2
from datetime import datetime

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    sys.exit("ERROR: LEAGUE_KEY not set")

CONFIG_FILE = "oauth2.json"
DAILY_PARQUET = "player_stats_full.parquet"

oauth = OAuth2(None, None, from_file=CONFIG_FILE)

BASE = "https://fantasysports.yahooapis.com/fantasy/v2"


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def get_json(url):
    r = oauth.session.get(url)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


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


def get_league_current_date():
    """
    Ask Yahoo what date the league considers 'current'.
    This is the ONLY safe date for daily stats.
    """
    url = f"{BASE}/league/{LEAGUE_KEY}?format=json"
    j = get_json(url)
    if not j:
        return None
    return find_first(j, "current_date")


# ---------------------------------------------------------
# Load player universe (league + roster)
# ---------------------------------------------------------
if not os.path.exists("league_players.csv"):
    sys.exit("league_players.csv not found")

league_players = pd.read_csv("league_players.csv", dtype=str)
player_keys = set(league_players["player_key"].dropna())

if os.path.exists("team_rosters.csv"):
    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    player_keys |= set(rosters["player_key"].dropna())

# Expand numeric keys to Yahoo canonical form
expanded_keys = set()
for k in player_keys:
    expanded_keys.add(k)
    if isinstance(k, str) and k.isdigit():
        expanded_keys.add(f"466.p.{k}")

player_keys = sorted(expanded_keys)
print(f"Total unique player keys to fetch: {len(player_keys)}")


# ---------------------------------------------------------
# Determine stats date
# ---------------------------------------------------------
stats_date = get_league_current_date()
if not stats_date:
    print("No league current_date found — exiting safely")
    sys.exit(0)

print("Using league current_date:", stats_date)


# ---------------------------------------------------------
# Fetch logic (daily → season fallback)
# ---------------------------------------------------------
def fetch_player_stats(player_key):
    rows = []

    # 1) Try DAILY stats
    daily_url = (
        f"{BASE}/player/{player_key}/stats;date={stats_date}?format=json"
    )
    j = get_json(daily_url)

    player_node = find_first(j, "player") if j else None
    player_stats = find_first(player_node, "player_stats") if player_node else None
    stats_node = find_first(player_stats, "stats") if player_stats else None

    if stats_node and isinstance(stats_node, dict) and "stat" in stats_node:
        stats = stats_node["stat"]
        if isinstance(stats, dict):
            stats = [stats]

        name = find_first(player_node, "name")
        player_name = name.get("full") if isinstance(name, dict) else None

        for s in stats:
            sid = find_first(s, "stat_id")
            val = find_first(s, "value")
            rows.append({
                "player_key": player_key,
                "player_name": player_name,
                "coverage": "date",
                "period": stats_date,
                "timestamp": stats_date,
                "stat_id": sid,
                "stat_value": val,
            })

        return rows  # DAILY SUCCESS

    # 2) FALLBACK: SEASON stats
    season_url = f"{BASE}/player/{player_key}/stats?format=json"
    j = get_json(season_url)

    player_node = find_first(j, "player") if j else None
    player_stats = find_first(player_node, "player_stats") if player_node else None
    stats_node = find_first(player_stats, "stats") if player_stats else None

    if stats_node and isinstance(stats_node, dict) and "stat" in stats_node:
        stats = stats_node["stat"]
        if isinstance(stats, dict):
            stats = [stats]

        name = find_first(player_node, "name")
        player_name = name.get("full") if isinstance(name, dict) else None
        season = find_first(player_stats, "season")

        for s in stats:
            sid = find_first(s, "stat_id")
            val = find_first(s, "value")
            rows.append({
                "player_key": player_key,
                "player_name": player_name,
                "coverage": "season",
                "period": season,
                "timestamp": stats_date,
                "stat_id": sid,
                "stat_value": val,
            })

    return rows


# ---------------------------------------------------------
# Run fetch
# ---------------------------------------------------------
all_rows = []

for i, pk in enumerate(player_keys, 1):
    print(f"[{i}/{len(player_keys)}] Fetching {pk}")
    try:
        all_rows.extend(fetch_player_stats(pk))
    except Exception as e:
        print("Failed for", pk, type(e).__name__, e)


# ---------------------------------------------------------
# Write output
# ---------------------------------------------------------
df = pd.DataFrame(all_rows)

if df.empty:
    print("WARNING: No stats returned by Yahoo")
else:
    df.sort_values(
        by=["coverage", "timestamp", "player_key", "stat_id"],
        inplace=True,
        ignore_index=True,
    )

df.to_parquet(DAILY_PARQUET, index=False)
print("Saved player_stats_full.parquet rows:", len(df))
