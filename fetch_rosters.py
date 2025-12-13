import os
import csv
import json
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def collapse(node):
    """
    Yahoo returns list-of-dicts.
    This collapses them into a single dict.
    """
    out = {}
    for item in node:
        if isinstance(item, dict):
            out.update(item)
    return out

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")

    print("Fetching raw league data …")
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    data = r.json()

    league = data["fantasy_content"]["league"]
    if isinstance(league, list):
        league = collapse(league)

    teams_node = league.get("teams")
    if isinstance(teams_node, list):
        teams_node = collapse(teams_node)

    teams = ensure_list(teams_node.get("team"))

    rows = []

    for team in teams:
        team_dict = collapse(team)

        team_key = team_dict.get("team_key")
        team_name = team_dict.get("name")

        print(f"Fetching roster for {team_key}")

        roster_url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
        rr = oauth.session.get(roster_url)
        rr.raise_for_status()
        rdata = rr.json()

        team_node = rdata["fantasy_content"]["team"]
        if isinstance(team_node, list):
            team_node = collapse(team_node)

        roster = team_node.get("roster", {})
        players = ensure_list(roster.get("players", {}).get("player"))

        for p in players:
            pdata = collapse(p)
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
