# fetch_team_roster_snapshot.py

import csv
import os
from datetime import datetime, timezone
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all, extract_fragment, extract_name

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TS = datetime.now(timezone.utc).isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")

url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
r = oauth.session.get(url)
r.raise_for_status()
data = r.json()

league = first_dict(find_all(data, "league"))
teams = find_all(league, "team")

rows = []

for team in teams:
    team_key = extract_fragment(team, "team_key")
    team_name = extract_name(team)

    roster_players = find_all(team, "player")
    for p in roster_players:
        rows.append({
            "timestamp": TS,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": extract_fragment(p, "player_key"),
            "player_name": extract_name(p),
        })

if not rows:
    print("WARNING: No roster rows extracted")

file_exists = os.path.exists("fact_team_roster_snapshot.csv")

with open("fact_team_roster_snapshot.csv", "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["timestamp", "team_key", "team_name", "player_key", "player_name"]
    )
    if not file_exists:
        writer.writeheader()
    writer.writerows(rows)

print(f"Appended {len(rows)} rows to fact_team_roster_snapshot.csv")
