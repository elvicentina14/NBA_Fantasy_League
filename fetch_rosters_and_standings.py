# fetch_rosters_and_standings.py
#
# Outputs:
#   - team_rosters.csv : team_key, team_name, player_key, player_name, position
#   - standings.csv    : team_key, team_name, rank, wins, losses, ties, win_pct
#
# Strategy:
#   - Rosters:
#       league/{LEAGUE_KEY}          -> find teams
#       team/{team_key}/roster       -> players per team
#   - Standings:
#       league/{LEAGUE_KEY}/standings -> ranks, W/L/T, pct
#
# If standings or teams are missing/odd, we log a WARNING but do NOT crash.
# That way the workflow continues and stats can still be updated.

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
    """Try hard to get a human-readable team name."""
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

    # ---------- ROSTERS (league -> teams -> team/{team_key}/roster) ---------- #

    print("Fetching league info to build team rosters...")
    league_data = safe_get_json(oauth, f"league/{LEAGUE_KEY}")
    if not league_data:
        print("WARNING: league/{LEAGUE_KEY} returned empty/invalid JSON; cannot build rosters.")
        teams_for_roster: List[Any] = []
    else:
        teams_node = safe_find(league_data, "teams")
        if isinstance(teams_node, dict) and "team" in teams_node:
            teams_for_roster = ensure_list(teams_node["team"])
        elif isinstance(teams_node, list):
            teams_for_roster = teams_node
        else:
            print("WARNING: Could not find 'teams' in league response; no rosters will be built.")
            teams_for_roster = []

    roster_rows: List[Dict[str, Any]] = []

    for t in teams_for_roster:
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
            # last-resort search
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

    # ---------- STANDINGS (league/{LEAGUE_KEY}/standings) ---------- #

    print("Fetching standings...")

    standings_data = safe_get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    if not standings_data:
        print("WARNING: standings API returned empty/invalid JSON; standings.csv will NOT be created.")
        return

    standings_root = safe_find(standings_data, "standings")
    teams_node = safe_find(standings_root, "teams") if standings_root else None

    if isinstance(teams_node, dict) and "team" in teams_node:
        teams_for_standings = ensure_list(teams_node["team"])
    elif isinstance(teams_node, list):
        teams_for_standings = teams_node
    else:
        print("WARNING: Could not find 'teams' in standings response; standings.csv will NOT be created.")
        return

    standings_rows: List[Dict[str, Any]] = []

    for t in teams_for_standings:
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
        # rank might be string; safe sort
        if "rank" in df_standings.columns:
            df_standings["rank_numeric"] = pd.to_numeric(df_standings["rank"], errors="coerce")
            df_standings.sort_values(by=["rank_numeric", "team_name"], inplace=True, ignore_index=True)
            df_standings.drop(columns=["rank_numeric"], inplace=True)

        df_standings.to_csv(STANDINGS_CSV, index=False)
        print(f"Wrote {len(df_standings)} rows to {STANDINGS_CSV}")
    else:
        print("WARNING: No standings rows built; standings.csv will NOT be created.")


if __name__ == "__main__":
    main()
