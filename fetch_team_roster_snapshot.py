# fetch_team_roster_snapshot.py
from yahoo_oauth import OAuth2
import os, csv
from datetime import datetime, timezone
from yahoo_normalize import first

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT = "fact_team_roster_snapshot.csv"
SNAPSHOT_TS = datetime.now(timezone.utc).isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")

rows = []

# get teams
teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
teams_data = oauth.session.get(teams_url).json()

league = first(teams_data["fantasy_content"]["league"])
teams = first(league["teams"]).get("team", [])

for raw_team in teams:
    team = first(raw_team)
    team_key = team.get("team_key")
    team_name = team.get("name")

    roster = first(team.get("roster"))
    players_block = first(roster.get("players"))
    player_items = players_block.get("player", [])

    for raw_player in player_items:
        p = first(raw_player)
        rows.append({
            "snapshot_ts": SNAPSHOT_TS,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": p.get("player_key"),
        })

file_exists = os.path.exists(OUT)

with open(OUT, "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    if not file_exists:
        writer.writeheader()
    writer.writerows(rows)

print(f"Appended {len(rows)} rows to {OUT}")
