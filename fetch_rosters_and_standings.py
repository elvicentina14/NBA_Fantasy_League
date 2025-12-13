from yahoo_oauth import OAuth2
import os
import pandas as pd

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def as_list(x):
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
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

def main():
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth invalid")

    # ---------- TEAMS ----------
    print("Fetching league teams...")
    teams = []
    start = 0

    while True:
        data = get_json(
            oauth,
            f"league/{LEAGUE_KEY}/teams;start={start};count=25"
        )

        node = data["fantasy_content"]["league"]["teams"]
        page = as_list(node.get("team"))

        if not page:
            break

        teams.extend(page)
        start += 25

    if not teams:
        raise SystemExit("No teams returned")

    print(f"Found {len(teams)} teams")

    # ---------- ROSTERS ----------
    roster_rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t["name"]

        print(f"→ {team_name}")

        data = get_json(oauth, f"team/{team_key}/roster")
        players = as_list(
            data["fantasy_content"]["team"]["roster"]["players"]["player"]
        )

        for p in players:
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": p["player_key"],
                "player_name": p["name"]["full"],
                "position": p.get("display_position")
            })

    pd.DataFrame(roster_rows).to_csv("team_rosters.csv", index=False)
    print(f"Wrote {len(roster_rows)} roster rows")

    # ---------- STANDINGS ----------
    print("Fetching standings...")
    data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    teams = as_list(
        data["fantasy_content"]["league"]["standings"]["teams"]["team"]
    )

    standings = []
    for t in teams:
        s = t["team_standings"]
        standings.append({
            "team_key": t["team_key"],
            "team_name": t["name"],
            "rank": s["rank"],
            "wins": s["outcome_totals"]["wins"],
            "losses": s["outcome_totals"]["losses"],
            "ties": s["outcome_totals"]["ties"],
            "pct": s["outcome_totals"]["percentage"]
        })

    pd.DataFrame(standings).to_csv("standings.csv", index=False)
    print(f"Wrote {len(standings)} standings rows")

if __name__ == "__main__":
    main()
