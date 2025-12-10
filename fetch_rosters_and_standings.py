# fetch_rosters_and_standings.py
#
# Outputs:
#   - team_rosters.csv : team_key, team_name, player_key, player_name, position
#   - standings.csv    : team_key, team_name, rank, wins, losses, ties, win_pct

import os
from json import JSONDecodeError
from typing import Any, Dict, List

import pandas as pd
from yahoo_oauth import OAuth2

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


# ---------------- helpers ---------------- #

def safe_find(obj: Any, key: str) -> Any:
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


def ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def safe_get_json(oauth: OAuth2, path: str) -> Dict[str, Any]:
    """Call Yahoo Fantasy API and force JSON format, return {} if decode fails or non-200."""
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
    except JSONDecodeError:
        print(f"JSON decode error for URL: {url} (status {resp.status_code})")
        return {}


def extract_team_name(team_node: Dict[str, Any]) -> str:
    name = None
    name_obj = team_node.get("name")
    if isinstance(name_obj, dict):
        name = name_obj.get("full") or safe_find(name_obj, "full")

    if not name:
        name = safe_find(team_node, "name") or safe_find(team_node, "full")

    return str(name or "Unknown team")


# ---------------- main ---------------- #

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("Missing oauth2.json")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid inside GitHub Actions – refresh locally first.")

    print("Fetching league standings (for teams + standings)...")
    league_standings = safe_get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    if not league_standings:
        raise SystemExit("Could not fetch league standings; no teams available for rosters.")

    standings_root = safe_find(league_standings, "standings")
    teams_node = safe_find(standings_root, "teams") if standings_root else None

    if isinstance(teams_node, dict) and "team" in teams_node:
        teams = ensure_list(teams_node["team"])
    elif isinstance(teams_node, list):
        teams = teams_node
    else:
        teams = []
        print("WARNING: Could not find 'teams' in league standings response.")

    if not teams:
        raise SystemExit("No teams found in standings; cannot build rosters/standings CSVs.")

    # ---------- ROSTERS ---------- #

    print("Building team_rosters.csv from team/{team_key}/roster calls...")

    roster_rows: List[Dict[str, Any]] = []

    for t in teams:
        team_key = safe_find(t, "team_key")
        team_name = extract_team_name(t)

        if not team_key:
            continue

        team_key = str(team_key)
        print(f"GET roster for team: {team_key} / {team_name}")

        team_data = safe_get_json(oauth, f"team/{team_key}/roster")
        if not team_data:
            print(f"Roster response empty/invalid for team {team_key}")
            continue

        roster_root = safe_find(team_data, "roster")
        if not roster_root:
            print(f"Roster parse fail for team {team_key}")
            continue

        players_node = safe_find(roster_root, "players")
        if isinstance(players_node, dict) and "player" in players_node:
            players = ensure_list(players_node["player"])
        elif isinstance(players_node, list):
            players = players_node
        else:
            players = ensure_list(safe_find(team_data, "player"))

        if not players:
            print(f"No players found in roster for team {team_key}")
            continue

        for p in players:
            pk = safe_find(p, "player_key")
            if not pk:
                continue
            name = safe_find(p, "full") or safe_find(p, "name") or "Unknown"
            pos = safe_find(p, "display_position")

            roster_rows.append(
                {
                    "team_key": team_key,
                    "team_name": team_name,
                    "player_key": str(pk),
                    "player_name": str(name),
                    "position": pos,
                }
            )

    if roster_rows:
        df_rosters = pd.DataFrame(roster_rows)
        df_rosters.drop_duplicates(subset=["team_key", "player_key"], inplace=True)
        df_rosters.sort_values(by=["team_key", "player_key"], inplace=True, ignore_index=True)
        df_rosters.to_csv(ROSTERS_CSV, index=False)
        print(f"Wrote {len(df_rosters)} rows to {ROSTERS_CSV}")
    else:
        print("WARNING: No roster rows built; team_rosters.csv will NOT be created.")

    # ---------- STANDINGS ---------- #

    print("Building standings.csv...")

    standings_rows: List[Dict[str, Any]] = []

    for t in teams:
        team_key = safe_find(t, "team_key")
        team_name = extract_team_name(t)

        rank = safe_find(t, "rank")
        wins = safe_find(t, "wins")
        losses = safe_find(t, "losses")
        ties = safe_find(t, "ties")
        pct = safe_find(t, "percentage")

        if not team_key:
            continue

        standings_rows.append(
            {
                "team_key": str(team_key),
                "team_name": team_name,
                "rank": rank,
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "win_pct": pct,
            }
        )

    if standings_rows:
        df_standings = pd.DataFrame(standings_rows)
        df_standings.sort_values(by="rank", inplace=True, ignore_index=True)
        df_standings.to_csv(STANDINGS_CSV, index=False)
        print(f"Wrote {len(df_standings)} rows to {STANDINGS_CSV}")
    else:
        print("WARNING: No standings rows built; standings.csv will NOT be created.")


if __name__ == "__main__":
    main()
