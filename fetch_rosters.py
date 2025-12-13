import os
import csv
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import League

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def list_to_dict(node):
    """Convert Yahoo list-of-dicts into one dict"""
    out = {}
    for item in node:
        if isinstance(item, dict):
            out.update(item)
    return out

def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    league = League(oauth, LEAGUE_KEY)

    print("Fetching teams via league.teams() …")
    teams = league.teams()

    rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t.get("name", "")

        print(f"Fetching roster for {team_key}")

        roster = league.to_team(team_key).roster()

        players = ensure_list(roster.get("players", {}).get("player"))

        for p in players:
            pdata = list_to_dict(p)
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": pdata.get("player_key"),
                "player_name": pdata.get("name", {}).get("full"),
                "position": pdata.get("display_position")
            })

    with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["team_key", "team_name", "player_key", "player_name", "position"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows → team_rosters.csv")

if __name__ == "__main__":
    main()
