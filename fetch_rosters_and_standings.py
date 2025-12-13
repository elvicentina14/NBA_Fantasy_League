from yahoo_oauth import OAuth2
import pandas as pd
import os

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def as_list(x):
    if isinstance(x, list):
        return x
    if x is None:
        return []
    return [x]

def unwrap(x):
    if isinstance(x, list):
        return x[0]
    return x

oauth = OAuth2(None, None, from_file="oauth2.json")

# -------------------------------------------------
# FETCH TEAMS (MOST RELIABLE ENDPOINT)
# -------------------------------------------------
print("Fetching league teams...")

resp = oauth.session.get(
    f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
).json()

league = unwrap(resp["fantasy_content"]["league"])
teams_node = league["teams"]["team"]
teams = as_list(teams_node)

print(f"Found {len(teams)} teams")

team_keys = []
team_names = {}

for t in teams:
    team = unwrap(t)
    team_keys.append(team["team_key"])
    team_names[team["team_key"]] = team["name"]

# -------------------------------------------------
# FETCH STANDINGS (OPTIONAL BUT NOW SAFE)
# -------------------------------------------------
print("Fetching standings...")

standings_rows = []

try:
    resp = oauth.session.get(
        f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/standings?format=json"
    ).json()

    league = unwrap(resp["fantasy_content"]["league"])
    standings = unwrap(league["standings"])
    teams_standings = as_list(standings["teams"]["team"])

    for t in teams_standings:
        team = unwrap(t)
        stats = team["team_standings"]["outcome_totals"]

        standings_rows.append({
            "team_key": team["team_key"],
            "team_name": team["name"],
            "rank": team["team_standings"]["rank"],
            "wins": stats["wins"],
            "losses": stats["losses"],
            "ties": stats["ties"],
            "pct": stats["percentage"],
        })

except Exception as e:
    print("⚠️ Standings not available:", e)

pd.DataFrame(standings_rows).to_csv("standings.csv", index=False)
print(f"✅ Wrote {len(standings_rows)} standings rows")

# -------------------------------------------------
# FETCH ROSTERS
# -------------------------------------------------
print("Fetching team rosters...")

roster_rows = []

for team_key in team_keys:
    print(f"→ {team_key}")

    resp = oauth.session.get(
        f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    ).json()

    team = unwrap(resp["fantasy_content"]["team"])
    players = as_list(unwrap(team["roster"])["players"]["player"])

    for p in players:
        player = unwrap(p)
        roster_rows.append({
            "team_key": team_key,
            "team_name": team_names.get(team_key),
            "player_key": player["player_key"],
            "player_name": player["name"]["full"],
            "position": player.get("display_position"),
        })

pd.DataFrame(roster_rows).to_csv("team_rosters.csv", index=False)
print(f"✅ Wrote {len(roster_rows)} roster rows")
