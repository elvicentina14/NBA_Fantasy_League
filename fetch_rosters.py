# fetch_rosters.py
from yahoo_oauth import OAuth2
import os
import pandas as pd
from json import JSONDecodeError

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

OUTPUT = "team_rosters.csv"


# ---------- helpers ----------

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def safe_find(obj, key):
    """Recursively find first occurrence of key in Yahoo JSON."""
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


def yahoo_get(oauth, path):
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


# ---------- main ----------

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    print("Fetching league data...")
    league_data = yahoo_get(oauth, f"league/{LEAGUE_KEY}")

    teams_node = safe_find(league_data, "teams")
    teams = ensure_list(safe_find(teams_node, "team"))

    if not teams:
        raise SystemExit("❌ No teams found in league response")

    print(f"Found {len(teams)} teams")

    rows = []

    for team in teams:
        team_key = safe_find(team, "team_key")
        team_name = safe_find(team, "name")

        if not team_key:
            continue

        print(f"Fetching roster → {team_key}")

        roster_data = yahoo_get(oauth, f"team/{team_key}/roster")
        players_node = safe_find(roster_data, "players")
        players = ensure_list(safe_find(players_node, "player"))

        for p in players:
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": safe_find(p, "player_key"),
                "player_name": safe_find(p, "full"),
                "position": safe_find(p, "display_position"),
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT, index=False)
    print(f"✅ Wrote {len(df)} rows → {OUTPUT}")


if __name__ == "__main__":
    main()
