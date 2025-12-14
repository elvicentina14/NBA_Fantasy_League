import os
import pandas as pd
from yahoo_oauth import OAuth2
from datetime import datetime

oauth = OAuth2(None, None, from_file="oauth2.json")

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

today = datetime.utcnow().date().isoformat()
rows = []


def fetch(pk):
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats;date={today}?format=json"
    r = oauth.session.get(url)
    if r.status_code != 200:
        return []
    try:
        j = r.json()
        player = j["fantasy_content"]["player"]
        name = next(i["name"]["full"] for i in player[0] if "name" in i)
        stats = player[1]["player_stats"]["stats"]["stat"]
        out = []
        for s in stats:
            out.append({
                "player_key": pk,
                "player_name": name,
                "timestamp": today,
                "stat_id": s["stat_id"],
                "stat_value": s["value"]
            })
        return out
    except Exception:
        return []


for i, pk in enumerate(player_keys, 1):
    print(f"[{i}/{len(player_keys)}] {pk}")
    rows.extend(fetch(pk))

pd.DataFrame(rows).to_parquet("player_stats_full.parquet", index=False)
print("Saved player_stats_full.parquet rows:", len(rows))
