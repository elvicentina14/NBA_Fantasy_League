# fetch_players.py

import csv
import os
import requests
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "league_players.csv"


def get_oauth():
    return OAuth2(None, None, from_file="oauth2.json")


def fetch_page(oauth, start):
    url = (
        f"https://fantasysports.yahooapis.com/fantasy/v2/"
        f"league/{LEAGUE_KEY}/players;start={start};count=25"
    )
    r = oauth.session.get(url, params={"format": "json"})
    r.raise_for_status()
    return r.json()


def extract_players(payload):
    league = first_dict(payload["fantasy_content"]["league"])
    players_block = league.get("players", {})
    players = as_list(players_block.get("player"))
    rows = []

    for p in players:
        pdata = first_dict(p)
        rows.append({
            "player_key": pdata.get("player_key"),
            "name": pdata.get("name", {}).get("full"),
            "editorial_team_abbr": pdata.get("editorial_team_abbr"),
            "position_type": pdata.get("position_type"),
        })

    return rows


def main():
    oauth = get_oauth()
    start = 0
    all_rows = []

    while True:
        data = fetch_page(oauth, start)
        rows = extract_players(data)

        if not rows:
            break

        all_rows.extend(rows)
        start += 25

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Wrote {len(all_rows)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
