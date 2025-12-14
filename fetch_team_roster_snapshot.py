# fetch_team_roster_snapshot.py
import csv
import os
from datetime import date
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"
SNAPSHOT_DATE = date.today().isoformat()
OUTFILE = "fact_team_roster_snapshot.csv"

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

def flatten(frags):
    out = {}
    for f in frags:
        if isinstance(f, dict):
            out.update(f)
    return out

# ---- fetch teams ----
league = session.get(
    f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json"
).json()

teams = league["fantasy_content"]["league"][1]["teams"]
team_keys = []

for i in range(int(teams["count"])):
    t = teams[str(i)]["team"][0][0]
    team_keys.append(t["team_key"])

rows = []

# ---- fetch rosters ----
for team_key in team_keys:
    r = session.get(
        f"{ROOT}/team/{team_key}/roster?format=json"
    ).json()

    team_block = r["fantasy_content"]["team"]
    team_meta = flatten(team_block[0])
    team_name = team_meta["name"]

    players = team_block[1]["roster"]["0"]["players"]

    for i in range(int(players["count"])):
        p = players[str(i)]["player"]
        meta = flatten(p)

        rows.append({
            "snapshot_date": SNAPSHOT_DATE,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": meta.get("player_key"),
            "player_name": meta.get("name", {}).get("full"),
            "position": meta.get("display_position")
        })

# ---- append ----
write_header = not os.path.exists(OUTFILE)

with open(OUTFILE, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    if write_header:
        w.writeheader()
    w.writerows(rows)

print(f"Appended {len(rows)} rows to {OUTFILE}")
