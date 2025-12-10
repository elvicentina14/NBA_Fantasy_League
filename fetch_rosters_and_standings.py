from yahoo_oauth import OAuth2
import os
import pandas as pd

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"

def find_first(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            f = find_first(v, key)
            if f is not None:
                return f
    elif isinstance(obj, list):
        for item in obj:
            f = find_first(item, key)
            if f is not None:
                return f
    return None

def get_json(oauth, rel):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + rel
    if "format=json" not in url:
        url += ("&" if "?" in url else "?") + "format=json"
    r = oauth.session.get(url)
    try:
        return r.json()
    except:
        return {}

def build_rosters(oauth):
    print("Fetching rosters...")
    data = get_json(oauth, f"league/{LEAGUE_KEY}/teams;out=roster")
    teams = find_first(data, "team")
    if not teams:
        print("No team data, skipping.")
        return pd.DataFrame()

    if not isinstance(teams, list):
        teams = [teams]

    rows = []

    for t in teams:
        team_key = find_first(t, "team_key")
        team_name = find_first(t, "full") or find_first(t, "name")
        players = find_first(t, "player")
        if not players:
            continue
        if not isinstance(players, list):
            players = [players]

        for p in players:
            pk = find_first(p, "player_key")
            name = find_first(p, "full")
            pos = find_first(p, "display_position")
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": pk,
                "player_name": name,
                "position": pos
            })

    df = pd.DataFrame(rows)
    df.to_csv(ROSTERS_CSV, index=False)
    print(f"Wrote {len(df)} rows -> {ROSTERS_CSV}")
    return df

def build_standings(oauth):
    print("Fetching standings...")
    data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    teams = find_first(data, "team")
    if not teams:
        print("No standings teams, skipping.")
        return pd.DataFrame()

    if not isinstance(teams, list):
        teams = [teams]

    rows = []
    for t in teams:
        rows.append({
            "team_key": find_first(t, "team_key"),
            "team_name": find_first(t, "full"),
            "rank": find_first(t, "rank"),
            "wins": find_first(t, "wins"),
            "losses": find_first(t, "losses"),
            "ties": find_first(t, "ties"),
            "win_pct": find_first(t, "percentage"),
        })

    df = pd.DataFrame(rows)
    df.to_csv(STANDINGS_CSV, index=False)
    print(f"Wrote {len(df)} rows -> {STANDINGS_CSV}")
    return df

if __name__ == "__main__":
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        print("Token expired, refresh locally.")
        exit(1)

    build_rosters(oauth)
    build_standings(oauth)
