# fetch_rosters_and_standings.py
#
# Robust Yahoo Fantasy extractor for:
# - team_rosters.csv
# - standings.csv
#
# Handles Yahoo's inconsistent list/dict nesting safely.

from yahoo_oauth import OAuth2
import os
import pandas as pd

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


# -------------------- helpers -------------------- #

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def deep_find(obj, key):
    """Find ALL dicts that contain `key` anywhere in structure."""
    found = []

    if isinstance(obj, dict):
        if key in obj:
            found.append(obj)
        for v in obj.values():
            found.extend(deep_find(v, key))

    elif isinstance(obj, list):
        for item in obj:
            found.extend(deep_find(item, key))

    return found


def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"

    resp = oauth.session.get(url)
    resp.raise_for_status()
    return resp.json()


def extract_name(node):
    if not isinstance(node, dict):
        return None

    name = node.get("name")
    if isinstance(name, dict):
        return name.get("full")
    return None


# -------------------- main -------------------- #

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    print("Fetching league standings (source of teams)...")
    data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")

    # ----------- TEAMS + STANDINGS ----------- #

    team_nodes = deep_find(data, "team_key")

    if not team_nodes:
        raise SystemExit("❌ No teams found in Yahoo response")

    standings_rows = []
    roster_rows = []

    print(f"Found {len(team_nodes)} teams")

    for team in team_nodes:
        if not isinstance(team, dict):
            continue

        team_key = team.get("team_key")
        if not team_key:
            continue

        team_name = extract_name(team)

        # standings fields
        standings_rows.append(
            {
                "team_key": team_key,
                "team_name": team_name,
                "rank": team.get("rank"),
                "wins": team.get("wins"),
                "losses": team.get("losses"),
                "ties": team.get("ties"),
                "pct": team.get("percentage"),
            }
        )

        # ----------- ROSTER ----------- #

        print(f"Fetching roster for {team_key}")
        roster_data = get_json(oauth, f"team/{team_key}/roster")

        player_nodes = deep_find(roster_data, "player_key")

        for p in player_nodes:
            if not isinstance(p, dict):
                continue

            player_key = p.get("player_key")
            if not player_key:
                continue

            roster_rows.append(
                {
                    "team_key": team_key,
                    "team_name": team_name,
                    "player_key": player_key,
                    "player_name": extract_name(p),
                    "position": p.get("display_position"),
                }
            )

    # ----------- WRITE FILES ----------- #

    df_rosters = pd.DataFrame(roster_rows).drop_duplicates(
        subset=["team_key", "player_key"]
    )
    df_rosters.to_csv(ROSTERS_CSV, index=False)
    print(f"✅ Wrote {len(df_rosters)} rows → {ROSTERS_CSV}")

    df_standings = pd.DataFrame(standings_rows).drop_duplicates(
        subset=["team_key"]
    )
    df_standings.to_csv(STANDINGS_CSV, index=False)
    print(f"✅ Wrote {len(df_standings)} rows → {STANDINGS_CSV}")


if __name__ == "__main__":
    main()
