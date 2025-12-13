import os
import pandas as pd
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
CONFIG = "oauth2.json"

def as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for i in obj:
            r = find(i, key)
            if r is not None:
                return r
    return None

def get(oauth, path):
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/{path}?format=json"
    return oauth.session.get(url).json()

def main():
    oauth = OAuth2(None, None, from_file=CONFIG)

    print("Fetching league standings (source of teams)...")
    data = get(oauth, f"league/{LEAGUE_KEY}/standings")
    teams = as_list(find(data, "team"))

    if not teams:
        raise SystemExit("❌ No teams found in standings")

    standings_rows = []
    roster_rows = []

    for t in teams:
        team_key = find(t, "team_key")
        team_name = find(t, "name")

        standings_rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "rank": find(t, "rank"),
            "wins": find(t, "wins"),
            "losses": find(t, "losses"),
            "ties": find(t, "ties"),
            "pct": find(t, "percentage")
        })

        print(f"Fetching roster for {team_key}")
        roster = get(oauth, f"team/{team_key}/roster")
        players = as_list(find(roster, "player"))

        for p in players:
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": find(p, "player_key"),
                "player_name": find(p, "full"),
                "position": find(p, "display_position")
            })

    pd.DataFrame(roster_rows).to_csv("team_rosters.csv", index=False)
    pd.DataFrame(standings_rows).to_csv("standings.csv", index=False)

    print("✅ team_rosters.csv and standings.csv written")

if __name__ == "__main__":
    main()
