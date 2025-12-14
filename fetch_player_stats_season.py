import os
import pandas as pd
from yahoo_oauth import OAuth2
from yahoo_utils import ensure_list, unwrap

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

players = pd.read_csv("league_players.csv")

rows = []

for i, p in players.iterrows():
    print(f"[{i+1}/{len(players)}] {p.player_key}")

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{p.player_key}/stats;type=season;season=2025?format=json"
    r = session.get(url)
    data = r.json()

    try:
        stats = data["fantasy_content"]["player"][1]["player_stats"]["stats"]
    except KeyError:
        continue

    for s in ensure_list(stats):
        stat = unwrap(s)["stat"]
        rows.append({
            "player_key": p.player_key,
            "stat_id": stat["stat_id"],
            "value": stat.get("value")
        })

df = pd.DataFrame(rows)
df.to_parquet("player_stats_full.parquet", index=False)

print(f"Saved player_stats_full.parquet rows: {len(df)}")
