import os
import pandas as pd
from datetime import date
from yahoo_oauth import OAuth2
from yahoo_utils import ensure_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TODAY = date.today().isoformat()

players = pd.read_csv("league_players.csv")

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

rows = []

for i, row in players.iterrows():
    pk = row["player_key"]
    print(f"[{i+1}/{len(players)}] Fetching {pk}")

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats;date={TODAY}?format=json"
    r = session.get(url).json()

    stats = r["fantasy_content"]["player"][1].get("player_stats", {}).get("stats", [])

    for stat in ensure_list(stats):
        s = stat.get("stat", {})
        rows.append({
            "player_key": pk,
            "date": TODAY,
            "stat_id": s.get("stat_id"),
            "value": s.get("value")
        })

df = pd.DataFrame(rows)
df.to_parquet("player_stats_daily.parquet", index=False)

print(f"Saved player_stats_daily.parquet rows: {len(df)}")
