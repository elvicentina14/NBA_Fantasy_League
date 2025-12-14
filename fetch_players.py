from yahoo_oauth import OAuth2
import os, csv

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

url = f"{ROOT}/league/{LEAGUE_KEY}/players;status=ALL?format=json"
data = s.get(url).json()

players_node = data["fantasy_content"]["league"][1]["players"]
count = int(players_node["count"])

rows = []

for i in range(count):
    player_wrapper = players_node[str(i)]["player"]

    # player_wrapper is a LIST
    # player_wrapper[0] is a LIST of dict fragments
    meta = player_wrapper[0][0]   # <-- THIS IS THE DICT

    rows.append({
        "player_key": meta["player_key"],
        "player_id": meta["player_id"],
        "editorial_player_key": meta["editorial_player_key"],
        "player_name": meta["name"]["full"],
    })

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "player_key",
            "player_id",
            "editorial_player_key",
            "player_name",
        ],
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote league_players.csv rows: {len(rows)}")
