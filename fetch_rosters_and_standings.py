from yahoo_oauth import OAuth2
import os
import pandas as pd
from json import JSONDecodeError

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


# ---------- helpers ----------

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = find_first(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for i in obj:
            found = find_first(i, key)
            if found is not None:
                return found
    return None


def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"

    r = oauth.session.get(url)
    r.raise_for_status()
    try:
        return r.json()
    except JSONDecodeError:
        return {}


def extract_teams(data):
    teams_container = find_first(data, "teams")
    teams = []

    if isinstance(teams_container, dict):
        if "team" in teams_container:
            teams = ensure_list(teams_container["team"])
        else:
            for v in teams_container.values():
                if isinstance(v, dict) and "team" in v:
                    teams.append(v["team"])

    elif isinstance(teams_container, list):
        teams = teams_container

    return teams


# ---------- main ----------

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth invalid")

    print("Fetching league teams...")
    league = get_json(oauth, f"league/{LEAGUE_KEY}")
    teams = extract_teams(league)

    if not teams:
        raise SystemExit("No teams found")

    print(f"Found {len(teams)} teams")

    # ---------- ROSTERS ----------
    roster_rows = []

    for t in teams:
        team_key = find_first(t, "team_key")
        team_name = find_first(t, "name")

        print(f"→ {team_name}")

        roster = get_json(oauth, f"team/{team_key}/roster")
        players = find_first(roster, "players")
        player_nodes = ensure_list(find_first(players, "player"))

        for p in player_nodes:
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": find_first(p, "player_key"),
                "player_name": find_first(p, "full"),
                "position": find_first(p, "display_position"),
            })

    pd.DataFrame(roster_rows).to_csv(ROSTERS_CSV, index=False)
    print(f"Wrote {len(roster_rows)} rows → {ROSTERS_CSV}")

    # ---------- STANDINGS ----------
    standings = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    standings_teams = extract_teams(standings)

    rows = []
    for t in standings_teams:
        rows.append({
            "team_key": find_first(t, "team_key"),
            "team_name": find_first(t, "name"),
            "rank": find_first(t, "rank"),
            "wins": find_first(t, "wins"),
            "losses": find_first(t, "losses"),
            "ties": find_first(t, "ties"),
            "pct": find_first(t, "percentage"),
        })

    pd.DataFrame(rows).to_csv(STANDINGS_CSV, index=False)
    print(f"Wrote {len(rows)} rows → {STANDINGS_CSV}")


if __name__ == "__main__":
    main()
