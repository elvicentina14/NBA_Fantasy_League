import csv
import datetime
import os
from yahoo_oauth import OAuth2
from yahoo_utils import list_to_dict, safe_get

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
RUN_TS = datetime.datetime.utcnow().isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session


def extract_roster_players(roster_block):
    """
    Handles BOTH:
    - roster -> players
    - roster -> date -> players
    """
    roster_dict = list_to_dict(roster_block)

    # direct players
    if "players" in roster_dict:
        return list_to_dict(roster_dict["players"]).get("player", [])

    # date-wrapped
    for v in roster_dict.values():
        if isinstance(v, dict) and "players" in v:
            return list_to_dict(v["players"]).get("player", [])

    return []


rows = []

teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
teams_data = session.get(teams_url).json()

league = list_to_dict(teams_data["fantasy_content"]["league"])
teams = list_to_dict(league["teams"]).get("team", [])

for t in teams:
    team_meta = list_to_dict(t["team"])
    team_key = team_meta["team_key"]

    roster_url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    roster_data = session.get(roster_url).json()

    team_block = list_to_dict(roster_data["fantasy_content"]["team"])
    roster_block = team_block.get("roster")

    players = extract_roster_players(roster_block)

    for p in players:
        meta = list_to_dict(p["player"])
        name = list_to_dict(meta.get("name"))

        rows.append({
            "snapshot_ts": RUN_TS,
            "team_key": team_key,
            "player_key": safe_get(meta, "player_key"),
            "player_name": safe_get(name, "full")
        })


file = "fact_team_roster_snapshot.csv"

if rows:
    write_header = not os.path.exists(file)
    with open(file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

print(f"Appended {len(rows)} rows to {file}")
