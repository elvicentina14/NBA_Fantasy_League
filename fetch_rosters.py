import os
import csv
from yahoo_oauth import OAuth2
from yahoo_utils import ensure_list, unwrap

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
teams = session.get(teams_url).json()["fantasy_content"]["league"][1]["teams"]

rows = []

for _, team in teams.items():
    team = unwrap(team)["team"][0]
    team_key = team["team_key"]
    team_name = team["name"]

    roster_url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    roster = session.get(roster_url).json()

    players = roster["fantasy_content"]["team"][1]["roster"]["players"]
    for p in ensure_list(players):
        if not isinstance(p, dict):
            continue
        player = p["player"][0]
        rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "player_key": player.get("player_key"),
            "player_name": player.get("name", {}).get("full")
        })

with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote team_rosters.csv rows: {len(rows)}")
