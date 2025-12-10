# fetch_rosters_and_standings.py
#
# Builds:
#   - team_rosters.csv  (team_key, team_name, player_key, player_name, position)
#   - standings.csv     (team_key, team_name, rank, wins, losses, ties, win_pct)

import os
import pandas as pd
from yahoo_oauth import OAuth2
from typing import Any, Optional

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


# ---------------- helpers ---------------- #

def safe_find(obj: Any, key: str) -> Optional[Any]:
    """Safely search nested dict/list for first occurrence of key."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = safe_find(v, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = safe_find(item, key)
            if result is not None:
                return result
    return None


def safe_get_json(oauth: OAuth2, path: str) -> dict:
    """
    Call Yahoo Fantasy API and force JSON format.
    Return {} on non-200 or JSON decode error (instead of crashing).
    """
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}format=json"

    resp = oauth.session.get(url)
    if resp.status_code != 200:
        print(f"Non-200 response for URL: {url} (status {resp.status_code})")
        return {}

    try:
        return resp.json()
    except ValueError:
        print(f"JSON decode error for URL: {url} (status {resp.status_code})")
        return {}


# ---------------- main ---------------- #

def main():
    # Basic checks
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("Missing oauth2.json")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid inside GitHub Actions – refresh locally first.")

    # ---------- ROSTERS ---------- #

    print("Fetching team rosters...")

    league_data = safe_get_json(oauth, f"league/{LEAGUE_KEY}")
    teams_node = safe_find(league_data, "teams")

    if not teams_node:
        print("Could not find 'teams' in league response – skipping rosters.")
    else:
        teams = teams_node.get("team") if isinstance(teams_node, dict) else teams_node
        if not isinstance(teams, list):
            teams = [teams]

        all_rows = []

        for t in teams:
            team_key = safe_find(t, "team_key")
            team_name = safe_find(t, "name")

            if not team_key:
                continue

            print(f"GET roster for team: {team_key} / {team_name}")

            team_data = safe_get_json(oauth, f"team/{team_key}/roster")
            roster_root = safe_find(team_data, "roster")

            if not roster_root:
                print(f"Roster parse fail for team {team_key}")
                continue

            players_node = safe_find(roster_root, "players")
            players = players_node.get("player") if isinstance(players_node, dict) else players_node

            if not isinstance(players, list):
                players = [players]

            for p in players or []:
                pk = safe_find(p, "player_key")
                name = safe_find(p, "full") or safe_find(p, "name")
                pos = safe_find(p, "display_position")

                if not pk:
                    continue

                all_rows.append(
                    {
                        "team_key": team_key,
                        "team_name": team_name,
                        "player_key": pk,
                        "player_name": name,
                        "position": pos,
                    }
                )

        df_rosters = pd.DataFrame(all_rows)
        df_rosters.to_csv(ROSTERS_CSV, index=False)
        print(f"Wrote {len(df_rosters)} rows to {ROSTERS_CSV}")

    # ---------- STANDINGS ---------- #

    print("Fetching standings...")

    standings_data = safe_get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    standings_root = safe_find(standings_data, "standings")

    if not standings_root:
        print("Could not find standings in API response – skipping standings.")
        return

    teams_node = safe_find(standings_root, "teams")
    teams = teams_node.get("team") if isinstance(teams_node, dict) else teams_node

    if not isinstance(teams, list):
        teams = [teams]

    standings_rows = []

    for t in teams:
        team_key = safe_find(t, "team_key")
        team_name = safe_find(t, "name")

        rank = safe_find(t, "rank")
        wins = safe_find(t, "wins")
        losses = safe_find(t, "losses")
        ties = safe_find(t, "ties")
        pct = safe_find(t, "percentage")

        standings_rows.append(
            {
                "team_key": team_key,
                "team_name": team_name,
                "rank": rank,
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "win_pct": pct,
            }
        )

    df_standings = pd.DataFrame(standings_rows)
    df_standings.to_csv(STANDINGS_CSV, index=False)
    print(f"Wrote {len(df_standings)} rows to {STANDINGS_CSV}")


if __name__ == "__main__":
    main()
