# fetch_players.py

import csv
import os
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all, extract_fragment, extract_name

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
PAGE_SIZE = 25

oauth = OAuth2(None, None, from_file="oauth2.json")

rows = []
start = 0

while True:
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players;start={start};count={PAGE_SIZE}?format=json"
    print("INFO GET", url)
    r = oauth.session.get(url)
    r.raise_for_status()
    data = r.json()

    league = first_dict(find_all(data, "league"))
    players_block = first_dict(find_all(league, "players"))

    if not players_block:
        start += PAGE_SIZE
        continue

    count = int(players_block.get("count", 0))
    if count == 0 and start > 0:
        break

    players = find_all(players_block, "player")
    if not players:
        start += PAGE_SIZE
        continue

    for p in players:
        meta = first_dict(p)
        rows.append({
            "player_key": extract_fragment(p, "player_key"),
            "player_id": extract_fragment(p, "player_id"),
            "editorial_player_key": extract_fragment(p, "editorial_player_key"),
            "player_name": extract_name(p),
        })

    start += PAGE_SIZE

if not rows:
    print("WARNING: 0 players extracted")

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["player_key", "player_id", "editorial_player_key", "player_name"]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to league_players.csv")
