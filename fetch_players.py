# fetch_players.py
import csv
import os
from yahoo_oauth import OAuth2
from yahoo_utils import merge_kv_list, as_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "league_players.csv"

def oauth():
    return OAuth2(None, None, from_file="oauth2.json")

def main():
    session = oauth().session
    start = 0
    rows = []

    while True:
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players;start={start};count=25"
        r = session.get(url, params={"format": "json"})
        r.raise_for_status()
        data = r.json()

        league = merge_kv_list(data["fantasy_content"]["league"])
        players_block = merge_kv_list(league["players"])
        players = as_list(players_block["player"])

        if not players:
            break

        for p in players:
            pdata = merge_kv_list(p)
            name = merge_kv_list(pdata["name"])

            rows.append({
                "player_key": pdata["player_key"],
                "player_id": pdata["player_id"],
                "player_name": name["full"],
                "editorial_team_abbr": pdata["editorial_team_abbr"],
                "position_type": pdata["position_type"],
            })

        start += 25

    if not rows:
        print("No players returned")
        return

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows")

if __name__ == "__main__":
    main()
