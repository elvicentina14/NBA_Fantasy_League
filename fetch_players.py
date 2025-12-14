import os
import csv
from yahoo_oauth import OAuth2
import requests
from yahoo_utils import as_list, first_dict, find_all

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

oauth = OAuth2(None, None, from_file="oauth2.json")

def yahoo_get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

all_rows = []
start = 0
page_size = 25

while True:
    url = (
        f"https://fantasysports.yahooapis.com/fantasy/v2/"
        f"league/{LEAGUE_KEY}/players;start={start};count={page_size}?format=json"
    )

    data = yahoo_get(url)
    league = first_dict(find_all(data, "league"))
    players_block = find_all(league, "player")

    if not players_block:
        if start == 0:
            print("No players returned on first page; aborting")
        break

    for p in players_block:
        name = first_dict(p.get("name"))
        row = {
            "player_key": p.get("player_key"),
            "player_id": p.get("player_id"),
            "editorial_player_key": p.get("editorial_player_key"),
            "player_name": name.get("full"),
        }
        all_rows.append(row)

    start += page_size

print(f"Wrote {len(all_rows)} rows to league_players.csv")

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    if not all_rows:
        f.write("")
    else:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)
