# fetch_players.py

import csv
import os
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, merge_kv_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "league_players.csv"


def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    start = 0
    rows = []

    while True:
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players;start={start};count=25"
        r = oauth.session.get(url, params={"format": "json"})
        r.raise_for_status()
        data = r.json()

        league = merge_kv_list(data["fantasy_content"]["league"])
        players_block = as_list(league.get("players"))

        if not players_block:
            break

        for p in players_block:
            player = merge_kv_list(p["player"])
            name = merge_kv_list(player.get("name"))

            rows.append({
                "player_key": player.get("player_key"),
                "player_id": player.get("player_id"),
                "player_name": name.get("full"),
                "editorial_team_abbr": player.get("editorial_team_abbr"),
                "position_type": player.get("position_type"),
            })

        start += 25

    if not rows:
        raise RuntimeError("No players returned from Yahoo")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
