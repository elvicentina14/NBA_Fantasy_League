from yahoo_oauth import OAuth2
import os, csv

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

URL = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players;status=ALL?format=json"
data = s.get(URL).json()

players = []
players_node = data["fantasy_content"]["league"][1]["players"]

for i in range(int(players_node["count"])):
    p = players_node[str(i)]["player"]
    meta = p[0]

    players.append({
        "player_key": meta.get("player_key"),
        "player_id": meta.get("player_id"),
        "editorial_player_key": meta.get("editorial_player_key"),
        "player_name": meta["name"]["full"]
    })

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=players[0].keys())
    w.writeheader()
    w.writerows(players)

print("Wrote league_players.csv rows:", len(players))
