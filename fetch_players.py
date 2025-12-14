# fetch_players.py
from yahoo_oauth import OAuth2
import os, csv
from yahoo_utils import as_list, first_dict

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT = "league_players.csv"

oauth = OAuth2(None, None, from_file="oauth2.json")

rows = []
start = 0
count = 25

while True:
    url = (
        f"https://fantasysports.yahooapis.com/fantasy/v2/"
        f"league/{LEAGUE_KEY}/players;start={start};count={count}?format=json"
    )
    data = oauth.session.get(url).json()

    league = as_list(data["fantasy_content"]["league"])
    if len(league) < 2:
        break

    players_container = first_dict(league[1])
    players_block = first_dict(players_container.get("players"))
    players = as_list(players_block.get("player"))

    if not players:
        break

    for p_raw in players:
        p = first_dict(p_raw)
        name = first_dict(p.get("name"))

        rows.append({
            "player_key": p.get("player_key"),
            "player_id": p.get("player_id"),
            "editorial_player_key": p.get("editorial_player_key"),
            "player_name": name.get("full"),
        })

    start += count

with open(OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote league_players.csv rows: {len(rows)}")
