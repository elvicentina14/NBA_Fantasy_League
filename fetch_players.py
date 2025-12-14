import os, csv
from yahoo_oauth import OAuth2
from yahoo_utils import iter_indexed_dict, merge_fragments

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

rows = []
start = 0
count = 25

while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;start={start};count={count}?format=json"
    j = s.get(url).json()

    players_node = j["fantasy_content"]["league"][1]["players"]
    batch = iter_indexed_dict(players_node)

    if not batch:
        break

    for p in batch:
        player_block = p["player"][0]     # list of fragments
        player = merge_fragments(player_block)

        rows.append({
            "player_key": player.get("player_key"),
            "player_id": player.get("player_id"),
            "editorial_player_key": player.get("editorial_player_key"),
            "player_name": player.get("name", {}).get("full"),
        })

    start += count

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=["player_key","player_id","editorial_player_key","player_name"]
    )
    w.writeheader()
    w.writerows(rows)

print(f"Wrote league_players.csv rows: {len(rows)}")
