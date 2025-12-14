from yahoo_oauth import OAuth2
import os, csv
from datetime import datetime, timezone
import pandas as pd

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
today = datetime.now(timezone.utc).date().isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

players = pd.read_csv("league_players.csv", dtype=str)

rows = []

for _, p in players.iterrows():
    pk = p["player_key"]

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats?format=json"
    data = s.get(url).json()

    player = data["fantasy_content"]["player"]

    # player[1] ALWAYS holds stats block
    stats_block = player[1]["player_stats"]
    stats_list = stats_block["stats"]["stat"]

    for stat in stats_list:
        rows.append({
            "snapshot_date": today,
            "player_key": pk,
            "player_name": p["player_name"],
            "stat_id": stat["stat_id"],
            "stat_value": stat.get("value")
        })

file = "fact_player_season_snapshot.csv"
exists = os.path.exists(file)

with open(file, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    if not exists:
        w.writeheader()
    w.writerows(rows)

print(f"Appended {len(rows)} rows to {file}")
