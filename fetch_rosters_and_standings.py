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

def unwrap(node):
    if isinstance(node, list):
        return node[0]
    return node

oauth = OAuth2(None, None, from_file="oauth2.json")

# ---------- STANDINGS ----------
print("Fetching league standings...")
resp = oauth.session.get(
    f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/standings?format=json"
).json()

league = unwrap(resp["fantasy_content"]["league"])
teams_node = unwrap(league["standings"])["teams"]["team"]
teams = as_list(teams_node)

standings_rows = []
team_keys = []

for t in teams:
    team = unwrap(t)
    team_keys.append(team["team_key"])
    standings_rows.append({
        "team_key": team["team_key"],
        "team_name": team["name"],
        "rank": team["team_standings"]["rank"],
        "wins": team["team_standings"]["outcome_totals"]["wins"],
        "losses": team["team_standings"]["outcome_totals"]["losses"],
        "ties": team["team_standings"]["outcome_totals"]["ties"],
        "pct": team["team_standings"]["outcome_totals"]["percentage"],
    })

pd.DataFrame(standings_rows).to_csv("standings.csv", index=False)
print(f"✅ Wrote {len(standings_rows)} standings rows")

# ---------- ROSTERS ----------
print("Fetching team rosters...")
roster_rows = []

for team_key in team_keys:
    resp = oauth.session.get(
        f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    ).json()

    team = unwrap(resp["fantasy_content"]["team"])
    team_name = team["name"]

    players = as_list(unwrap(team["roster"])["players"]["player"])

    for p in players:
        player = unwrap(p)
        roster_rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "player_key": player["player_key"],
            "player_name": player["name"]["full"],
            "position": player.get("display_position"),
        })

pd.DataFrame(roster_rows).to_csv("team_rosters.csv", index=False)
print(f"✅ Wrote {len(roster_rows)} roster rows")
