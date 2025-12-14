from yahoo_oauth import OAuth2
import os, pandas as pd
from datetime import datetime, timedelta

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
DAILY_DIR = "player_stats_daily"

oauth = OAuth2(None, None, from_file="oauth2.json")
os.makedirs(DAILY_DIR, exist_ok=True)

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
print("Total player keys to fetch:", len(player_keys))

def fetch(pk, date):
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats;date={date}?format=json"
    r = oauth.session.get(url)
    if r.status_code != 200:
        return []
    j = r.json()
    stats = []
    try:
        player = j["fantasy_content"]["player"]
        name = next(x["name"]["full"] for x in player[0] if "name" in x)
        stats_node = player[1]["player_stats"]["stats"]["stat"]
        for s in stats_node:
            stats.append({
                "player_key": pk,
                "player_name": name,
                "timestamp": date,
                "stat_id": s["stat_id"],
                "stat_value": s["value"]
            })
    except Exception:
        pass
    return stats

today = datetime.utcnow().date().isoformat()
rows = []

for i,pk in enumerate(player_keys,1):
    print(f"[{i}/{len(player_keys)}] {pk}")
    rows.extend(fetch(pk, today))

df = pd.DataFrame(rows)
df.to_parquet("player_stats_full.parquet", index=False)
print("Saved player_stats_full.parquet rows:", len(df))
