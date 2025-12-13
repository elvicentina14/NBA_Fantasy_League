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

    # ======================================================
    # 1) GET TEAMS *FROM STANDINGS* (THIS IS THE FIX)
    # ======================================================
    print("Fetching teams via standings (reliable endpoint)...")

    data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")

    fc = unwrap(data["fantasy_content"])
    league = unwrap(fc["league"])
    standings = unwrap(league["standings"])
    teams_node = unwrap(standings["teams"])
    teams = as_list(teams_node["team"])

    if not teams:
        raise SystemExit("Yahoo returned zero teams")

    print(f"Found {len(teams)} teams")

    # ======================================================
    # 2) FETCH ROSTERS
    # ======================================================
    roster_rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t["name"]

        print(f"→ Fetching roster: {team_name}")

        data = get_json(oauth, f"team/{team_key}/roster")
        fc = unwrap(data["fantasy_content"])
        team = unwrap(fc["team"])
        roster = unwrap(team["roster"])
        players_node = unwrap(roster["players"])
        players = as_list(players_node["player"])

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
    print(f"Wrote {len(df_rosters)} rows → team_rosters.csv")

    # ======================================================
    # 3) BUILD STANDINGS CSV
    # ======================================================
    standings_rows = []

    for t in teams:
        s = t["team_standings"]
        ot = s["outcome_totals"]

        standings_rows.append({
            "team_key": t["team_key"],
            "team_name": t["name"],
            "rank": s["rank"],
            "wins": ot["wins"],
            "losses": ot["losses"],
            "ties": ot["ties"],
            "pct": ot["percentage"],
        })

    df_standings = pd.DataFrame(standings_rows)
    df_standings.to_csv("standings.csv", index=False)
    print(f"Wrote {len(df_standings)} rows → standings.csv")

if __name__ == "__main__":
    main()
