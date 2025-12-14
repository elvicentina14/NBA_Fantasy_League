from yahoo_oauth import OAuth2
import os, csv
from datetime import datetime, timezone

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

timestamp = datetime.now(timezone.utc).isoformat()

url = f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json"
data = s.get(url).json()

teams_node = data["fantasy_content"]["league"][1]["teams"]
team_count = int(teams_node["count"])

rows = []

for i in range(team_count):
    team_fragments = teams_node[str(i)]["team"][0]

    team_key = None
    team_name = None
    roster_players = []

    for frag in team_fragments:
        if not isinstance(frag, dict):
            continue

        if "team_key" in frag:
            team_key = frag["team_key"]

        if "name" in frag:
            team_name = frag["name"]

        if "roster" in frag:
            players = frag["roster"]["0"]["players"]
            for j in range(int(players["count"])):
                p_fragments = players[str(j)]["player"][0]
                for p in p_fragments:
                    if "player_key" in p:
                        roster_players.append(p["player_key"])

    if not team_key:
        raise RuntimeError(f"Missing team_key at index {i}")

    for player_key in roster_players:
        rows.append({
            "timestamp": timestamp,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": player_key,
        })

file_exists = os.path.exists("fact_team_roster_snapshot.csv")

with open("fact_team_roster_snapshot.csv", "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["timestamp", "team_key", "team_name", "player_key"]
    )
    if not file_exists:
        writer.writeheader()
    writer.writerows(rows)

print(f"Appended {len(rows)} rows to fact_team_roster_snapshot.csv")
