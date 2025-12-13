from yahoo_oauth import OAuth2
import os
import pandas as pd
from json import JSONDecodeError
from typing import Any, List

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


# ---------- helpers ----------

def ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj: Any, key: str) -> Any:
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


def get_json(oauth: OAuth2, path: str):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"

    r = oauth.session.get(url)
    r.raise_for_status()

    try:
        return r.json()
    except JSONDecodeError:
        print("JSON decode error:", url)
        return {}


# ---------- main ----------

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("oauth2.json missing")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    # ======================================================
    # 1. FETCH TEAMS (SAFE WAY)
    # ======================================================
    print("Fetching league teams...")

    teams_json = get_json(oauth, f"league/{LEAGUE_KEY}/teams")
    team_nodes = ensure_list(find_first(teams_json, "team"))

    if not team_nodes:
        raise SystemExit("No teams found in league response")

    teams = []
    for t in team_nodes:
        team_key = find_first(t, "team_key")
        team_name = find_first(t, "name")
        if team_key:
            teams.append({
                "team_key": team_key,
                "team_name": team_name
            })

    print(f"Found {len(teams)} teams")

    # ======================================================
    # 2. FETCH ROSTERS
    # ======================================================
    print("Fetching team rosters...")

    roster_rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t["team_name"]

        print(f"→ {team_name}")

        roster_json = get_json(oauth, f"team/{team_key}/roster")
        players = ensure_list(find_first(roster_json, "player"))

        for p in players:
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": find_first(p, "player_key"),
                "player_name": find_first(p, "full"),
                "position": find_first(p, "display_position"),
            })

    df_rosters = pd.DataFrame(roster_rows)
    df_rosters.to_csv(ROSTERS_CSV, index=False)
    print(f"Wrote {len(df_rosters)} rows → {ROSTERS_CSV}")

    # ======================================================
    # 3. FETCH STANDINGS
    # ======================================================
    print("Fetching standings...")

    standings_json = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    team_nodes = ensure_list(find_first(standings_json, "team"))

    rows = []
    for t in team_nodes:
        rows.append({
            "team_key": find_first(t, "team_key"),
            "team_name": find_first(t, "name"),
            "rank": find_first(t, "rank"),
            "wins": find_first(t, "wins"),
            "losses": find_first(t, "losses"),
            "ties": find_first(t, "ties"),
            "win_pct": find_first(t, "percentage"),
        })

    df_standings = pd.DataFrame(rows)
    df_standings.to_csv(STANDINGS_CSV, index=False)
    print(f"Wrote {len(df_standings)} rows → {STANDINGS_CSV}")


if __name__ == "__main__":
    main()
