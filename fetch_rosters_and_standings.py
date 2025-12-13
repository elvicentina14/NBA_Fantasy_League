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

def unwrap(x):
    while isinstance(x, list) and len(x) > 0:
        x = x[0]
    return x

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

    # ---------- FETCH TEAMS ----------
    print("Fetching league teams...")
    teams = []
    start = 0

    while True:
        data = get_json(
            oauth,
            f"league/{LEAGUE_KEY}/teams;start={start};count=25"
        )

        fc = unwrap(data.get("fantasy_content"))
        league = unwrap(fc.get("league"))
        teams_node = unwrap(league.get("teams"))

        page = as_list(teams_node.get("team"))
        if not page:
            break

        teams.extend(page)
        start += 25

    if not teams:
        raise SystemExit("No teams returned by Yahoo")

    print(f"Found {len(teams)} teams")

    # ---------- FETCH ROSTERS ----------
    roster_rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t["name"]

        print(f"→ {team_name}")

        data = get_json(oauth, f"team/{team_key}/roster")
        fc = unwrap(data.get("fantasy_content"))
        team = unwrap(fc.get("team"))
        roster = unwrap(team.get("roster"))
        players_node = unwrap(roster.get("players"))

        players = as_list(players_node.get("player"))

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
    print(f"Wrote {len(df_rosters)} roster rows → team_rosters.csv")

    # ---------- FETCH STANDINGS ----------
    print("Fetching standings...")

    data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    fc = unwrap(data.get("fantasy_content"))
    league = unwrap(fc.get("league"))
    standings = unwrap(league.get("standings"))
    teams_node = unwrap(standings.get("teams"))

    teams = as_list(teams_node.get("team"))

    rows = []
    for t in teams:
        s = t["team_standings"]
        ot = s["outcome_totals"]

        rows.append({
            "team_key": t["team_key"],
            "team_name": t["name"],
            "rank": s["rank"],
            "wins": ot["wins"],
            "losses": ot["losses"],
            "ties": ot["ties"],
            "pct": ot["percentage"],
        })

    df_standings = pd.DataFrame(rows)
    df_standings.to_csv("standings.csv", index=False)
    print(f"Wrote {len(df_standings)} rows → standings.csv")

if __name__ == "__main__":
    main()
