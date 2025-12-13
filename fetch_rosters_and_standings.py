from yahoo_oauth import OAuth2
import os
import pandas as pd
from json import JSONDecodeError

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


# ---------------- helpers ---------------- #

def safe_find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = safe_find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for i in obj:
            r = safe_find(i, key)
            if r is not None:
                return r
    return None


def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def get_json(oauth, path):
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


# ---------------- main ---------------- #

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth invalid")

    print("Fetching league teams...")
    league = get_json(oauth, f"league/{LEAGUE_KEY}")
    teams_node = safe_find(league, "team")
    teams = ensure_list(teams_node)

    if not teams:
        raise SystemExit("No teams found")

    # ---------- ROSTERS ---------- #
    roster_rows = []

    for t in teams:
        team_key = safe_find(t, "team_key")
        team_name = safe_find(t, "name")

        if not team_key:
            continue

        print(f"→ {team_name}")

        roster = safe_find(t, "roster")
        players = ensure_list(safe_find(roster, "player"))

        for p in players:
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": safe_find(p, "player_key"),
                "player_name": safe_find(p, "full"),
                "position": safe_find(p, "display_position"),
            })

    df_rosters = pd.DataFrame(roster_rows)
    df_rosters.to_csv(ROSTERS_CSV, index=False)
    print(f"Wrote {len(df_rosters)} rows → {ROSTERS_CSV}")

    # ---------- STANDINGS ---------- #
    standings_rows = []

    for t in teams:
        standings = safe_find(t, "team_standings")
        if not standings:
            continue

        standings_rows.append({
            "team_key": safe_find(t, "team_key"),
            "team_name": safe_find(t, "name"),
            "rank": safe_find(standings, "rank"),
            "wins": safe_find(standings, "wins"),
            "losses": safe_find(standings, "losses"),
            "ties": safe_find(standings, "ties"),
            "pct": safe_find(standings, "percentage"),
        })

    df_standings = pd.DataFrame(standings_rows)
    df_standings.to_csv(STANDINGS_CSV, index=False)
    print(f"Wrote {len(df_standings)} rows → {STANDINGS_CSV}")


if __name__ == "__main__":
    main()
