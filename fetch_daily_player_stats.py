# fetch_daily_player_stats.py
import os, pandas as pd
from datetime import datetime
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

players = pd.read_csv("league_players.csv", dtype=str)
today = datetime.utcnow().date().isoformat()

out = []

for i, pk in enumerate(players["player_key"], start=1):
    print(f"[{i}/{len(players)}] {pk}")
    url = f"{ROOT}/player/{pk}/stats;date={today}?format=json"
    r = oauth.session.get(url)
    if r.status_code != 200:
        continue

    j = r.json()
    stats = None
    try:
        stats = j["fantasy_content"]["player"][1]["player_stats"]["stats"]["stat"]
    except Exception:
        continue

    for s in stats:
        out.append({
            "player_key": pk,
            "date": today,
            "stat_id": s["stat_id"],
            "value": s.get("value")
        })

df = pd.DataFrame(out)
os.makedirs("player_stats_daily", exist_ok=True)
df.to_csv(f"player_stats_daily/{today}.csv", index=False)
df.to_parquet("player_stats_full.parquet", index=False)

print("Saved player_stats_full.parquet rows:", len(df))
