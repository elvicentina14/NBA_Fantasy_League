# fetch_players.py
from yahoo_oauth import OAuth2
import os, csv
from yahoo_normalize import first

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT = "league_players.csv"

oauth = OAuth2(None, None, from_file="oauth2.json")

players = []
start = 0
count = 25

while True:
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players;start={start};count={count}?format=json"
    data = oauth.session.get(url).json()

    league = first(data["fantasy_content"]["league"])
    players_block = first(league["players"])
    player_items = players_block.get("player", [])

    if not player_items:
        break

    for raw in player_items:
        p = first(raw)
        name = first(p.get("name"))
        players.append({
            "player_key": p.get("player_key"),
            "player_id": p.get("player_id"),
            "editorial_player_key": p.get("editorial_player_key"),
            "player_name": name.get("full"),
        })

    start += count

with open(OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=players[0].keys())
    writer.writeheader()
    writer.writerows(players)

print(f"Wrote {len(players)} rows to {OUT}")
