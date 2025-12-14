import os
import sys
import pandas as pd
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    sys.exit("ERROR: LEAGUE_KEY not set")

BASE = "https://fantasysports.yahooapis.com/fantasy/v2"
OUT = "player_stats_full.parquet"

oauth = OAuth2(None, None, from_file="oauth2.json")


# ---------------- helpers ----------------
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


def get_current_week():
    j = get_json(f"{BASE}/league/{LEAGUE_KEY}?format=json")
    if not j:
        return None
    return find_first(j, "current_week")


# ---------------- load players ----------------
lp = pd.read_csv("league_players.csv", dtype=str)
keys = set(lp["player_key"].dropna())

if os.path.exists("team_rosters.csv"):
    r = pd.read_csv("team_rosters.csv", dtype=str)
    keys |= set(r["player_key"].dropna())

expanded = set()
for k in keys:
    expanded.add(k)
    if k.isdigit():
        expanded.add(f"466.p.{k}")

player_keys = sorted(expanded)
print("Total player keys:", len(player_keys))


# ---------------- determine week ----------------
current_week = get_current_week()
if not current_week:
    sys.exit("Could not determine current_week")

print("Using fantasy week:", current_week)


# ---------------- fetch stats ----------------
rows = []

for i, pk in enumerate(player_keys, 1):
    print(f"[{i}/{len(player_keys)}] Fetching {pk}")

    # 1) WEEKLY fantasy stats (PRIMARY)
    week_url = (
        f"{BASE}/player/{pk}/stats;type=week;week={current_week}?format=json"
    )
    j = get_json(week_url)

    player = find_first(j, "player") if j else None
    stats = find_first(player, "stats") if player else None

    if stats and "stat" in stats:
        name = find_first(player, "name")
        pname = name.get("full") if isinstance(name, dict) else None

        stat_list = stats["stat"]
        if isinstance(stat_list, dict):
            stat_list = [stat_list]

        for s in stat_list:
            rows.append({
                "player_key": pk,
                "player_name": pname,
                "coverage": "week",
                "period": current_week,
                "stat_id": s.get("stat_id"),
                "stat_value": s.get("value"),
            })
        continue  # success, skip fallback

    # 2) FALLBACK: SEASON TOTALS
    season_url = f"{BASE}/player/{pk}/stats;type=season?format=json"
    j = get_json(season_url)

    player = find_first(j, "player") if j else None
    stats = find_first(player, "stats") if player else None

    if stats and "stat" in stats:
        name = find_first(player, "name")
        pname = name.get("full") if isinstance(name, dict) else None

        stat_list = stats["stat"]
        if isinstance(stat_list, dict):
            stat_list = [stat_list]

        for s in stat_list:
            rows.append({
                "player_key": pk,
                "player_name": pname,
                "coverage": "season",
                "period": "season",
                "stat_id": s.get("stat_id"),
                "stat_value": s.get("value"),
            })


# ---------------- write output ----------------
df = pd.DataFrame(rows)
df.to_parquet(OUT, index=False)

print("Saved player_stats_full.parquet rows:", len(df))
