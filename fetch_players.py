import os
import csv
from yahoo_oauth import OAuth2
import requests
from yahoo_utils import ensure_list, unwrap

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players"
params = {"format": "json", "status": "ALL", "count": 25}

rows = []
start = 0

while True:
    params["start"] = start
    r = session.get(url, params=params)
    data = r.json()

    players = data["fantasy_content"]["league"][1]["players"]
    total = players["count"]

    for p in ensure_list(players.values())[1:]:
        p = unwrap(p)["player"]
        meta = unwrap(p)[0]

        rows.append({
            "player_key": meta["player_key"],
            "player_id": meta["player_id"],
            "editorial_player_key": meta["editorial_player_key"],
            "player_name": meta["name"]["full"]
        })

    start += 25
    if start >= total:
        break

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote league_players.csv rows: {len(rows)}")
