# fetch_rosters.py
from yahoo_oauth import OAuth2
import os
import pandas as pd

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
OUT_FILE = "team_rosters.csv"


def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def yahoo_get(oauth, path):
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/{path}?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()


def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    print("Fetching teams via league/{league_key}/teams …")

    data = yahoo_get(oauth, f"league/{LEAGUE_KEY}/teams")

    # Yahoo response shape is ALWAYS:
    # fantasy_content → league → 1 → teams → team
    league = data["fantasy_content"]["league"]
    league_data = league[1]
    teams = ensure_list(league_data["teams"]["team"])

    print(f"Found {len(teams)} teams")

    rows = []

    for t in teams:
        team_key = t["team_key"]
        team_name = t.get("name")

        print(f"Fetching roster → {team_key}")
        roster_data = yahoo_get(oauth, f"team/{team_key}/roster")

        players = ensure_list(
            roster_data["fantasy_content"]["team"][1]["roster"]["players"]["player"]
        )

        for p in players:
            name = p.get("name", {})
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": p.get("player_key"),
                "player_name": name.get("full"),
                "position": p.get("display_position"),
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUT_FILE, index=False)
    print(f"✅ Wrote {len(df)} rows → {OUT_FILE}")


if __name__ == "__main__":
    main()
