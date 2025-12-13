import os
import csv
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import League

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def find_teams(node, found):
    if isinstance(node, dict):
        if "team_key" in node:
            found.append(node)
        for v in node.values():
            find_teams(v, found)
    elif isinstance(node, list):
        for i in node:
            find_teams(i, found)

def find_players(node, found):
    if isinstance(node, dict):
        if "player_key" in node:
            found.append(node)
        for v in node.values():
            find_players(v, found)
    elif isinstance(node, list):
        for i in node:
            find_players(i, found)

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    league = League(oauth, LEAGUE_KEY)

    print("Fetching raw league data ...")
    raw = league.league_raw

    teams = []
    find_teams(raw, teams)

    rows = []

    for team in teams:
        team_key = team.get("team_key")
        team_name = team.get("name", "")

        try:
            team_obj = league.to_team(team_key)
            roster_raw = team_obj.roster_raw
        except Exception:
            continue

        players = []
        find_players(roster_raw, players)

        for p in players:
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": p.get("player_key"),
                "player_name": p.get("name", {}).get("full", ""),
                "position": ",".join(p.get("eligible_positions", []))
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
