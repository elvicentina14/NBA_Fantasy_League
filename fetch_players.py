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
    fragments = players_node[str(i)]["player"][0]

    player_key = None
    editorial_player_key = None
    player_id = None
    player_name = None

    for frag in fragments:
        if not isinstance(frag, dict):
            continue

        if "player_key" in frag:
            player_key = frag["player_key"]
            editorial_player_key = frag.get("editorial_player_key")
            player_id = frag.get("player_id")

        if "name" in frag and "full" in frag["name"]:
            player_name = frag["name"]["full"]

    if not player_key or not player_name:
        raise RuntimeError(f"Malformed player object at index {i}")

    rows.append({
        "player_key": player_key,
        "editorial_player_key": editorial_player_key,
        "player_id": player_id,
        "player_name": player_name,
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
