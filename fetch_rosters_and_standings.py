# fetch_rosters_and_standings.py
#
# What this does
# --------------
# 1. Fetches team rosters into team_rosters.csv with columns:
#       team_key, team_name, player_key, player_name, position
# 2. Fetches league standings into standings.csv with columns:
#       team_key, team_name, rank, wins, losses, ties, win_pct
#
# Design:
# - Uses the same LEAGUE_KEY + oauth2.json as fetch_full_player_stats.py
# - If rosters call completely fails (no rows), it DOES NOT write team_rosters.csv
#   so your existing fetch_full_player_stats.py will safely fall back to league_players.

from __future__ import annotations

import os
from typing import Any, Dict, List

import pandas as pd
from yahoo_oauth import OAuth2

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"


# ---------------- helpers (compatible with your other script) ---------------- #

def ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj: Any, key: str) -> Any:
    """Recursively search nested dict/list for first occurrence of key."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = find_first(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_first(item, key)
            if found is not None:
                return found
    return None


def get_json(oauth: OAuth2, relative_path: str) -> Dict[str, Any]:
    """
    Call Yahoo Fantasy API with ?format=json appended.

    Returns {} instead of throwing if Yahoo returns non-200 or non-JSON.
    """
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + relative_path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}format=json"

    resp = oauth.session.get(url)
    if resp.status_code != 200:
        print(f"Non-200 response for URL {url}: {resp.status_code}")
        return {}
    try:
        return resp.json()
    except ValueError:
        print(f"JSON decode error for URL: {url} (status {resp.status_code})")
        return {}


def _extract_team_name_from_node(team_node: Dict[str, Any]) -> str:
    """Best-effort extraction of a team's full name from a Yahoo team node."""
    name_obj = team_node.get("name")
    full_name = None

    if isinstance(name_obj, dict) and "full" in name_obj:
        full_name = name_obj["full"]
    elif isinstance(name_obj, dict):
        full_name = find_first(name_obj, "full")

    if not full_name:
        # Fallback: any 'name'/'full' deeper in the node
        full_name = find_first(team_node, "full") or find_first(team_node, "name")

    if not full_name:
        full_name = "Unknown team"

    return str(full_name)


def _extract_player_name_from_node(player_node: Dict[str, Any]) -> str:
    """Best-effort extraction of player full name."""
    name_obj = player_node.get("name") or find_first(player_node, "name")

    full_name = None
    if isinstance(name_obj, dict) and "full" in name_obj:
        full_name = name_obj["full"]
    elif isinstance(name_obj, dict):
        full_name = find_first(name_obj, "full")

    if not full_name:
        full_name = find_first(player_node, "full")

    if not full_name:
        full_name = "Unknown"

    return str(full_name)


# ---------------- rosters ---------------- #

def fetch_team_rosters(oauth: OAuth2) -> pd.DataFrame:
    """
    Build a DataFrame with:
        team_key, team_name, player_key, player_name, position
    from league/{LEAGUE_KEY}/teams;out=roster
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    print("Fetching team rosters from Yahoo...")

    data = get_json(oauth, f"league/{LEAGUE_KEY}/teams;out=roster")
    if not data:
        print("No data from league teams endpoint; returning empty rosters DataFrame.")
        return pd.DataFrame(
            columns=["team_key", "team_name", "player_key", "player_name", "position"]
        )

    teams_node = find_first(data, "teams")
    if not teams_node:
        print("Could not find 'teams' in league teams response.")
        return pd.DataFrame(
            columns=["team_key", "team_name", "player_key", "player_name", "position"]
        )

    if isinstance(teams_node, dict):
        teams = teams_node.get("team")
    else:
        teams = teams_node

    teams = ensure_list(teams)

    rows: List[Dict[str, str]] = []

    for t in teams:
        team_key = find_first(t, "team_key")
        if not team_key:
            continue
        team_key = str(team_key)
        team_name = _extract_team_name_from_node(t)

        roster_node = find_first(t, "roster")
        if not roster_node:
            print(f"No roster found for team {team_key} / {team_name}")
            continue

        players_node = find_first(roster_node, "players")
        if not players_node:
            print(f"No players array found for team {team_key} / {team_name}")
            continue

        if isinstance(players_node, dict):
            players = players_node.get("player")
        else:
            players = players_node

        players = ensure_list(players)

        for p in players:
            player_key = find_first(p, "player_key")
            if not player_key:
                continue
            player_key = str(player_key)
            player_name = _extract_player_name_from_node(p)
            position = find_first(p, "display_position")

            rows.append(
                {
                    "team_key": team_key,
                    "team_name": team_name,
                    "player_key": player_key,
                    "player_name": player_name,
                    "position": position,
                }
            )

    if not rows:
        print("WARNING: Did not find any (team, player) rows in rosters.")
        return pd.DataFrame(
            columns=["team_key", "team_name", "player_key", "player_name", "position"]
        )

    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["team_key", "player_key"], inplace=True)
    df.sort_values(by=["team_key", "player_key"], inplace=True, ignore_index=True)
    print(f"Built team_rosters DataFrame with {len(df)} rows.")
    return df


# ---------------- standings ---------------- #

def fetch_standings(oauth: OAuth2) -> pd.DataFrame:
    """
    Build a standings DataFrame with columns:
        team_key, team_name, rank, wins, losses, ties, win_pct
    from league/{LEAGUE_KEY}/standings
    """
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    print("Fetching league standings from Yahoo...")

    data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    if not data:
        print("No data from league standings endpoint.")
        return pd.DataFrame(
            columns=["team_key", "team_name", "rank", "wins", "losses", "ties", "win_pct"]
        )

    standings_node = find_first(data, "standings")
    if not standings_node:
        print("Could not find 'standings' node in API response.")
        return pd.DataFrame(
            columns=["team_key", "team_name", "rank", "wins", "losses", "ties", "win_pct"]
        )

    teams_node = find_first(standings_node, "teams")
    if not teams_node:
        print("Could not find 'teams' under standings.")
        return pd.DataFrame(
            columns=["team_key", "team_name", "rank", "wins", "losses", "ties", "win_pct"]
        )

    if isinstance(teams_node, dict):
        teams = teams_node.get("team")
    else:
        teams = teams_node

    teams = ensure_list(teams)

    rows: List[Dict[str, Any]] = []

    for t in teams:
        team_key = find_first(t, "team_key")
        if not team_key:
            continue
        team_key = str(team_key)
        team_name = _extract_team_name_from_node(t)

        rank = find_first(t, "rank")
        wins = find_first(t, "wins")
        losses = find_first(t, "losses")
        ties = find_first(t, "ties")
        pct = find_first(t, "percentage")

        rows.append(
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

    if not rows:
        print("WARNING: Did not find any team rows in standings.")
        return pd.DataFrame(
            columns=["team_key", "team_name", "rank", "wins", "losses", "ties", "win_pct"]
        )

    df = pd.DataFrame(rows)
    df.sort_values(by="rank", inplace=True, ignore_index=True)
    print(f"Built standings DataFrame with {len(df)} rows.")
    return df


# ---------------- main ---------------- #

def main() -> None:
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"{CONFIG_FILE} not found (must be {CONFIG_FILE})")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid – refresh locally first.")

    # --- rosters --- #
    df_rosters = fetch_team_rosters(oauth)
    if not df_rosters.empty:
        df_rosters.to_csv(ROSTERS_CSV, index=False)
        print(f"Wrote {len(df_rosters)} rows to {ROSTERS_CSV}")
    else:
        print("team_rosters DataFrame is empty; NOT writing team_rosters.csv")

    # --- standings --- #
    df_standings = fetch_standings(oauth)
    if not df_standings.empty:
        df_standings.to_csv(STANDINGS_CSV, index=False)
        print(f"Wrote {len(df_standings)} rows to {STANDINGS_CSV}")
    else:
        print("standings DataFrame is empty; NOT writing standings.csv")


if __name__ == "__main__":
    main()
