from yahoo_oauth import OAuth2
import pandas as pd
import os
from yahoo_api import yahoo_get

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def ensure_list(x):
    if isinstance(x, list):
        return x
    return [x]

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")

    print("Fetching teams...")
    data = yahoo_get(oauth, f"league/{LEAGUE_KEY}/teams")

    teams = ensure_list(data["league"][1]["teams"]["team"])
    print(f"Found {len(teams)} teams")

    rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t["name"]

        print(f"Fetching roster for {team_name}")
        roster = yahoo_get(oauth, f"team/{team_key}/roster")

        players = ensure_list(
            roster["team"][1]["roster"]["players"]["player"]
        )

        for p in players:
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": p["player_key"],
                "player_name": p["name"]["full"]
            })

    df = pd.DataFrame(rows)
    df.to_csv("team_rosters.csv", index=False)
    print(f"✅ Wrote {len(df)} rows → team_rosters.csv")

if __name__ == "__main__":
    main()
