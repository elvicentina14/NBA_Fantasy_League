# fetch_rosters.py
import os, csv, time
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

def find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            x = find(v, key)
            if x is not None:
                return x
    elif isinstance(obj, list):
        for i in obj:
            x = find(i, key)
            if x is not None:
                return x
    return None

# fetch teams
teams_json = get(f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json")
teams_node = find(teams_json, "teams")

rows = []

for i in range(int(teams_node.get("count", 0))):
    team_entry = teams_node.get(str(i))
    if not team_entry:
        continue

    team = team_entry.get("team", [])
    team_key = find(team, "team_key")
    team_name = find(team, "name")

    roster_json = get(f"{ROOT}/team/{team_key}/roster?format=json")
    players_node = find(roster_json, "players")

    if not isinstance(players_node, dict):
        continue

    for p in players_node.values():
        # 🚨 THIS IS THE IMPORTANT GUARD
        if not isinstance(p, dict):
            continue
        if "player" not in p:
            continue

        wrapper = p["player"][0]

        rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "player_key": find(wrapper, "player_key"),
            "player_name": find(wrapper, "full"),
            "position": find(wrapper, "display_position")
        })

    time.sleep(0.15)

with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["team_key", "team_name", "player_key", "player_name", "position"]
    )
    writer.writeheader()
    writer.writerows(rows)

print("Wrote team_rosters.csv rows:", len(rows))
