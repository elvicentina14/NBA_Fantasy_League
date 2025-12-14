import os, csv, sys
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

teams = get(f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json")
teams_node = teams["fantasy_content"]["league"][1]["teams"]

rows = []

for k, t in teams_node.items():
    if not k.isdigit():
        continue

    team_key = t["team"][0][0]["team_key"]
    team_name = t["team"][0][2]["name"]

    roster = get(f"{ROOT}/team/{team_key}/roster?format=json")
    players = roster["fantasy_content"]["team"][1]["roster"]["0"]["players"]

    for _, p in players.items():
        plist = p["player"][0]
        row = {
            "team_key": team_key,
            "team_name": team_name,
            "player_key": None,
            "player_name": None,
        }

        for item in plist:
            if not isinstance(item, dict):
                continue
            if "player_key" in item:
                row["player_key"] = item["player_key"]
            if "name" in item:
                row["player_name"] = item["name"]["full"]

        if row["player_key"]:
            rows.append(row)

with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f, fieldnames=["team_key", "team_name", "player_key", "player_name"]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote team_rosters.csv rows: {len(rows)}")
