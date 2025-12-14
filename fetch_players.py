# fetch_players.py
import csv
import os
import time
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

def flatten(frags):
    out = {}
    for f in frags:
        if isinstance(f, dict):
            out.update(f)
    return out

rows = []
start = 0
count = 25

while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;start={start};count={count}?format=json"
    r = session.get(url).json()

    players = (
        r["fantasy_content"]["league"][1]["players"]
    )

    if str(start) not in players:
        print(f"No players found on page start={start}; stopping")
        break

    for i in range(int(players["count"])):
        p = players[str(i)]["player"]
        meta = flatten(p)

        rows.append({
            "player_key": meta.get("player_key"),
            "player_id": meta.get("player_id"),
            "editorial_player_key": meta.get("editorial_player_key"),
            "player_name": meta.get("name", {}).get("full")
        })

    start += count
    time.sleep(0.2)

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)

print(f"Wrote {len(rows)} rows to league_players.csv")
