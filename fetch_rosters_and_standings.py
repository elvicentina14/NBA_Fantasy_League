from yahoo_oauth import OAuth2
import pandas as pd
import os
from typing import Any, List

CONFIG = "oauth2.json"
LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def find_all(obj, key, out):
    if isinstance(obj, dict):
        if key in obj:
            out.append(obj[key])
        for v in obj.values():
            find_all(v, key, out)
    elif isinstance(obj, list):
        for i in obj:
            find_all(i, key, out)

def yahoo(oauth, path):
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/{path}?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

oauth = OAuth2(None, None, from_file=CONFIG)

# ---------- STANDINGS ----------
data = yahoo(oauth, f"league/{LEAGUE_KEY}/standings")

teams = []
find_all(data, "team", teams)

standings_rows = []
team_keys = []

for t in teams:
    team_key = t.get("team_key")
    name = t.get("name", {}).get("full")

    s = t.get("team_standings", {}).get("outcome_totals", {})

    standings_rows.append({
        "team_key": team_key,
        "team_name": name,
        "wins": s.get("wins"),
        "losses": s.get("losses"),
        "ties": s.get("ties"),
        "pct": s.get("percentage"),
        "rank": t.get("team_standings", {}).get("rank"),
    })

    team_keys.append(team_key)

pd.DataFrame(standings_rows).to_csv("standings.csv", index=False)

# ---------- ROSTERS ----------
roster_rows = []

for tk in team_keys:
    d = yahoo(oauth, f"team/{tk}/roster")
    players = []
    find_all(d, "player", players)

    for p in players:
        roster_rows.append({
            "team_key": tk,
            "team_name": p.get("editorial_team_full_name"),
            "player_key": p.get("player_key"),
            "player_name": p.get("name", {}).get("full"),
            "position": p.get("display_position"),
        })

pd.DataFrame(roster_rows).to_csv("team_rosters.csv", index=False)
print("✔ rosters + standings written")
