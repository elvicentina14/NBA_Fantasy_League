import os
import csv
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import League, Team

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    league = League(oauth, LEAGUE_KEY)

    print("Fetching teams via league.teams() ...")
    team_keys = league.teams()   # ✅ returns list[str]

    rows = []

    for team_key in team_keys:
        team = Team(oauth, team_key)

        team_name = team.team_name  # ✅ PROPERTY, NOT METHOD

        roster = team.roster()      # ✅ list of dicts

        for p in roster:
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": p["player_key"],
                "player_name": p["name"]["full"],
                "position": ",".join(p.get("eligible_positions", []))
            })

    with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "team_key",
                "team_name",
                "player_key",
                "player_name",
                "position"
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows → team_rosters.csv")

if __name__ == "__main__":
    main()
