from yahoo_oauth import OAuth2
import os
import pandas as pd
from json import JSONDecodeError

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


def safe_find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = safe_find(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = safe_find(item, key)
            if found is not None:
                return found
    return None


def safe_get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"

    resp = oauth.session.get(url)
    resp.raise_for_status()

    try:
        return resp.json()
    except JSONDecodeError:
        return {}


def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)

    # -------- ROSTERS --------
    league = safe_get_json(oauth, f"league/{LEAGUE_KEY}")
    teams_node = safe_find(league, "teams")
    teams = teams_node.get("team") if isinstance(teams_node, dict) else teams_node

    roster_rows = []

    for t in teams:
        team_key = safe_find(t, "team_key")
        team_name = safe_find(t, "name")

        team_data = safe_get_json(oauth, f"team/{team_key}/roster")
        roster = safe_find(team_data, "roster")
        players_node = safe_find(roster, "players")
        players = players_node.get("player") if isinstance(players_node, dict) else players_node

        for p in players:
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": safe_find(p, "player_key"),
                "player_name": safe_find(p, "full"),
                "position": safe_find(p, "display_position")
            })

    pd.DataFrame(roster_rows).to_csv(ROSTERS_CSV, index=False)

    # -------- STANDINGS --------
    standings_json = safe_get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    teams_node = safe_find(standings_json, "teams")
    teams = teams_node.get("team") if isinstance(teams_node, dict) else teams_node

    standings_rows = []

    for t in teams:
        standings_rows.append({
            "team_key": safe_find(t, "team_key"),
            "team_name": safe_find(t, "name"),
            "rank": safe_find(t, "rank"),
            "wins": safe_find(t, "wins"),
            "losses": safe_find(t, "losses"),
            "ties": safe_find(t, "ties"),
            "win_pct": safe_find(t, "percentage"),
        })

    pd.DataFrame(standings_rows).to_csv(STANDINGS_CSV, index=False)


if __name__ == "__main__":
    main()
