# fetch_team_roster_snapshot.py
import csv
import os
from datetime import datetime
from yahoo_oauth import OAuth2
from yahoo_utils import merge_kv_list, as_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "fact_team_roster_snapshot.csv"

def oauth():
    return OAuth2(None, None, from_file="oauth2.json")

def main():
    session = oauth().session
    ts = datetime.utcnow().isoformat()

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams"
    r = session.get(url, params={"format": "json"})
    r.raise_for_status()
    data = r.json()

    league = merge_kv_list(data["fantasy_content"]["league"])
    teams_block = merge_kv_list(league["teams"])
    teams = as_list(teams_block["team"])

    rows = []

    for t in teams:
        team = merge_kv_list(t)
        team_key = team["team_key"]
        team_name = team["name"]

        roster = merge_kv_list(team["roster"])
        players_block = merge_kv_list(roster["players"])
        players = as_list(players_block["player"])

        for p in players:
            pdata = merge_kv_list(p)
            name = merge_kv_list(pdata["name"])

            rows.append({
                "snapshot_ts": ts,
                "team_key": team_key,
                "team_name": team_name,
                "player_key": pdata["player_key"],
                "player_name": name["full"],
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

    print(f"Appended {len(rows)} rows")

if __name__ == "__main__":
    main()
