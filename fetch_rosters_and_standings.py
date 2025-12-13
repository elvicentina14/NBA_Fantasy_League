from yahoo_oauth import OAuth2
import os
import pandas as pd

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
            r = find_first(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for i in obj:
            r = find_first(i, key)
            if r is not None:
                return r
    return None


def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"
    resp = oauth.session.get(url)
    resp.raise_for_status()
    return resp.json()


# ---------- main ----------

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth invalid")

    # ================= ROSTERS =================

    print("Fetching league teams with rosters...")
    data = get_json(oauth, f"league/{LEAGUE_KEY}/teams;out=roster")

    teams = find_first(data, "team")
    teams = ensure_list(teams)

    if not teams:
        raise SystemExit("No teams found (rosters)")

    print(f"Found {len(teams)} teams")

    roster_rows = []

    for t in teams:
        team_key = find_first(t, "team_key")
        team_name = find_first(t, "name")

        print(f"→ {team_name}")

        players = find_first(t, "player")
        for p in ensure_list(players):
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

    # ================= STANDINGS =================

    print("Fetching standings...")
    standings = get_json(oauth, f"league/{LEAGUE_KEY}/standings")

    teams = ensure_list(find_first(standings, "team"))
    if not teams:
        raise SystemExit("No teams found (standings)")

    rows = []
    for t in teams:
        rows.append({
            "team_key": find_first(t, "team_key"),
            "team_name": find_first(t, "name"),
            "rank": find_first(t, "rank"),
            "wins": find_first(t, "wins"),
            "losses": find_first(t, "losses"),
            "ties": find_first(t, "ties"),
            "pct": find_first(t, "percentage"),
        })

    df_standings = pd.DataFrame(rows)
    df_standings.to_csv(STANDINGS_CSV, index=False)
    print(f"Wrote {len(df_standings)} rows → {STANDINGS_CSV}")


if __name__ == "__main__":
    main()
