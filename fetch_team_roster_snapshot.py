from yahoo_oauth import OAuth2
import os, csv
from datetime import datetime, timezone

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
today = datetime.now(timezone.utc).date().isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

league = s.get(
    f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
).json()

teams = league["fantasy_content"]["league"][1]["teams"]

rows = []

for i in range(int(teams["count"])):
    team = teams[str(i)]["team"]
    team_key = team[0]["team_key"]
    team_name = team[0]["name"]

    roster = s.get(
        f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    ).json()

    players = roster["fantasy_content"]["team"][1]["roster"]["0"]["players"]

    for p in players.values():
        meta = p["player"][0]
        rows.append({
            "snapshot_date": today,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": meta["player_key"],
            "player_name": meta["name"]["full"]
        })

file = "fact_team_roster_snapshot.csv"
exists = os.path.exists(file)

with open(file, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    if not exists:
        w.writeheader()
    w.writerows(rows)

print(f"Appended {len(rows)} rows to {file}")
