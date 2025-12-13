from yahoo_oauth import OAuth2
import pandas as pd
import os

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

oauth = OAuth2(None, None, from_file="oauth2.json")

def as_list(x):
    return x if isinstance(x, list) else [x]

print("Fetching league teams...")

resp = oauth.session.get(
    f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
).json()

league = resp["fantasy_content"]["league"]
teams = as_list(league[1]["teams"]["team"])

print(f"Found {len(teams)} teams")

rows = []

for t in teams:
    team_key = t["team_key"]
    team_name = t["name"]

    print(f"→ Fetching roster for {team_name}")

    roster_resp = oauth.session.get(
        f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    ).json()

    team = roster_resp["fantasy_content"]["team"]
    players = as_list(team[1]["roster"]["players"]["player"])

    for p in players:
        rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "player_key": p["player_key"],
            "player_name": p["name"]["full"],
            "position": p.get("display_position"),
        })

df = pd.DataFrame(rows)
df.to_csv("team_rosters.csv", index=False)

print(f"✅ Wrote {len(df)} roster rows")
