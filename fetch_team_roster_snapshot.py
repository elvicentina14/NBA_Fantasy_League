import os
import csv
from datetime import datetime, timezone
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TS = datetime.now(timezone.utc).isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")

def yahoo_get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

url = (
    f"https://fantasysports.yahooapis.com/fantasy/v2/"
    f"league/{LEAGUE_KEY}/teams/roster?format=json"
)

data = yahoo_get(url)

rows = []

teams = find_all(data, "team")
for team in teams:
    team_key = team.get("team_key")
    team_name = first_dict(team.get("name")).get("full")

    roster_players = find_all(team, "player")
    for p in roster_players:
        name = first_dict(p.get("name"))
        rows.append({
            "timestamp": TS,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": p.get("player_key"),
            "player_id": p.get("player_id"),
            "player_name": name.get("full"),
        })

with open("fact_team_roster_snapshot.csv", "a", newline="", encoding="utf-8") as f:
    if rows:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if f.tell() == 0:
            writer.writeheader()
        writer.writerows(rows)

print(f"Appended {len(rows)} rows to fact_team_roster_snapshot.csv")
