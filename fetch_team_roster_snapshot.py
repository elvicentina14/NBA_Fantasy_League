# fetch_team_roster_snapshot.py

import csv
import os
from datetime import datetime
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, merge_kv_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "fact_team_roster_snapshot.csv"


def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    snapshot_ts = datetime.utcnow().isoformat()
    rows = []

    # 1. Get teams
    league_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams"
    r = oauth.session.get(league_url, params={"format": "json"})
    r.raise_for_status()
    data = r.json()

    league = merge_kv_list(data["fantasy_content"]["league"])
    teams = as_list(league.get("teams"))

    for t in teams:
        team = merge_kv_list(t["team"])
        team_key = team.get("team_key")
        team_name = team.get("name")

        # 2. Explicit roster endpoint (THIS is why it now works)
        roster_url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster"
        rr = oauth.session.get(roster_url, params={"format": "json"})
        rr.raise_for_status()
        roster_data = rr.json()

        team_block = merge_kv_list(roster_data["fantasy_content"]["team"])
        roster = merge_kv_list(team_block.get("roster"))
        players = as_list(roster.get("players"))

        for p in players:
            player = merge_kv_list(p["player"])
            name = merge_kv_list(player.get("name"))

            rows.append({
                "snapshot_ts": snapshot_ts,
                "team_key": team_key,
                "team_name": team_name,
                "player_key": player.get("player_key"),
                "player_name": name.get("full"),
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
