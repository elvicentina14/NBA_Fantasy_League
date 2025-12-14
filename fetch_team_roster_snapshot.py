# fetch_team_roster_snapshot.py

import csv
import os
from datetime import datetime
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "fact_team_roster_snapshot.csv"


def get_oauth():
    return OAuth2(None, None, from_file="oauth2.json")


def main():
    oauth = get_oauth()

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams"
    r = oauth.session.get(url, params={"format": "json"})
    r.raise_for_status()
    data = r.json()

    league = first_dict(data["fantasy_content"]["league"])
    teams = as_list(league.get("teams", {}).get("team"))

    rows = []
    snapshot_ts = datetime.utcnow().isoformat()

    for team in teams:
        team_dict = first_dict(team)
        team_key = team_dict.get("team_key")
        team_name = team_dict.get("name")

        roster_block = team_dict.get("roster", {})
        players = as_list(roster_block.get("players", {}).get("player"))

        for p in players:
            pdata = first_dict(p)
            rows.append({
                "snapshot_ts": snapshot_ts,
                "team_key": team_key,
                "team_name": team_name,
                "player_key": pdata.get("player_key"),
                "player_name": pdata.get("name", {}).get("full"),
            })

    if not rows:
        print("No roster rows found")
        return

    file_exists = os.path.exists(OUT_FILE)

    with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"Appended {len(rows)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
