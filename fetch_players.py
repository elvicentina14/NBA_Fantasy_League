import os
import csv
from yahoo_oauth import OAuth2
import requests
from yahoo_utils import extract_players, unwrap

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players;status=ALL?format=json"

players = []
start = 0
count = 25

while True:
    r = session.get(url.replace("players;", f"players;start={start};count={count};"))
    data = r.json()
    league = data["fantasy_content"]["league"][1]
    container = league["players"]

    batch = extract_players(container)
    if not batch:
        break

    players.extend(batch)
    start += count

rows = []
for p in players:
    p = unwrap(p)
    row = p.get("player", [{}])[0]
    rows.append({
        "player_key": row.get("player_key"),
        "player_id": row.get("player_id"),
        "editorial_player_key": row.get("editorial_player_key"),
        "player_name": row.get("name", {}).get("full")
    })

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote league_players.csv rows: {len(rows)}")
