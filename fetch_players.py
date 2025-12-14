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

    # player_wrapper[0] is a LIST of fragments
    # fragment[0] is the canonical identity dict
    meta = player_wrapper[0][0]

    rows.append({
        "player_key": meta["player_key"],                       # guaranteed
        "editorial_player_key": meta.get("editorial_player_key"),  # optional
        "player_id": meta.get("player_id"),                     # optional
        "player_name": meta["name"]["full"],                    # guaranteed
    })

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "player_key",
            "editorial_player_key",
            "player_id",
            "player_name",
        ],
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote league_players.csv rows: {len(rows)}")
