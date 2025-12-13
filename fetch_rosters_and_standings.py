from yahoo_oauth import OAuth2
import os
import pandas as pd

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ["LEAGUE_KEY"]

# ---------------- helpers ---------------- #

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def find_all_teams(obj, out):
    """
    Recursively find dicts that look like Yahoo 'team' objects.
    """
    if isinstance(obj, dict):
        if "team_key" in obj and "name" in obj:
            out.append(obj)
        for v in obj.values():
            find_all_teams(v, out)
    elif isinstance(obj, list):
        for item in obj:
            find_all_teams(item, out)

def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

# ---------------- main ---------------- #

def main():
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth invalid")

    print("Fetching league data (searching for teams anywhere in response)...")

    data = get_json(oauth, f"league/{LEAGUE_KEY}/scoreboard")

    teams = []
    find_all_teams(data, teams)

    # Deduplicate by team_key
    seen = {}
    for t in teams:
        seen[t["team_key"]] = t

    teams = list(seen.values())

    if not teams:
        raise SystemExit("❌ Yahoo returned no teams anywhere in response")

    print(f"✅ Found {len(teams)} teams")

    # ---------------- ROSTERS ---------------- #

    roster_rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t["name"]

        print(f"→ Fetching roster for {team_name}")

        data = get_json(oauth, f"team/{team_key}/roster")

        players = []
        find_all_players(data, players)

        for p in players:
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": p["player_key"],
                "player_name": p["name"]["full"],
                "position": p.get("display_position"),
            })

    df_rosters = pd.DataFrame(roster_rows)
    df_rosters.to_csv("team_rosters.csv", index=False)
    print(f"📝 Wrote {len(df_rosters)} rows → team_rosters.csv")

    # ---------------- STANDINGS ---------------- #

    standings_rows = []

    for t in teams:
        s = t.get("team_standings", {})
        ot = s.get("outcome_totals", {})

        standings_rows.append({
            "team_key": t["team_key"],
            "team_name": t["name"],
            "rank": s.get("rank"),
            "wins": ot.get("wins"),
            "losses": ot.get("losses"),
            "ties": ot.get("ties"),
            "pct": ot.get("percentage"),
        })

    df_standings = pd.DataFrame(standings_rows)
    df_standings.to_csv("standings.csv", index=False)
    print(f"📝 Wrote {len(df_standings)} rows → standings.csv")


def find_all_players(obj, out):
    if isinstance(obj, dict):
        if "player_key" in obj and "name" in obj:
            out.append(obj)
        for v in obj.values():
            find_all_players(v, out)
    elif isinstance(obj, list):
        for item in obj:
            find_all_players(item, out)


if __name__ == "__main__":
    main()
