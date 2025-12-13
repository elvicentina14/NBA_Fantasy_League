from yahoo_oauth import OAuth2
import os
import pandas as pd
from typing import Any

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

ROSTERS_CSV = "team_rosters.csv"
STANDINGS_CSV = "standings.csv"
PLAYERS_CSV = "league_players.csv"


# ---------------- helpers ---------------- #

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj: Any, key: str):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = find_first(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for i in obj:
            found = find_first(i, key)
            if found is not None:
                return found
    return None


def get_json(oauth, path):
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url += f"{sep}format=json"

    r = oauth.session.get(url)
    if r.status_code != 200:
        print("Non-200:", r.status_code)
        return {}

    try:
        return r.json()
    except Exception:
        return {}


def extract_team_name(team):
    name = team.get("name")
    if isinstance(name, dict):
        return name.get("full") or name.get("short")
    return str(name)


def extract_player_name(player):
    name = player.get("name")
    if isinstance(name, dict):
        return name.get("full")
    return str(name)


# ---------------- main ---------------- #

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var missing")
    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("oauth2.json missing")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth invalid")

    # ---------- STEP 1: GET TEAMS ----------
    print("Fetching league teams...")
    league_data = get_json(oauth, f"league/{LEAGUE_KEY}/teams")

    teams_node = find_first(league_data, "teams")
    teams = ensure_list(teams_node.get("team")) if isinstance(teams_node, dict) else []

    if not teams:
        raise SystemExit("Yahoo returned NO teams (this is a Yahoo issue, not your code)")

    print(f"Found {len(teams)} teams")

    roster_rows = []
    player_rows = []

    # ---------- STEP 2: PER-TEAM ROSTERS ----------
    print("Fetching team rosters...")
    for team in teams:
        team_key = team.get("team_key")
        team_name = extract_team_name(team)

        print(f"→ {team_name}")

        roster_data = get_json(oauth, f"team/{team_key}/roster")
        players_node = find_first(roster_data, "players")
        players = ensure_list(players_node.get("player")) if isinstance(players_node, dict) else []

        for p in players:
            player_key = p.get("player_key")
            player_name = extract_player_name(p)
            position = find_first(p, "display_position")

            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": player_key,
                "player_name": player_name,
                "position": position
            })

            player_rows.append({
                "player_key": player_key,
                "player_name": player_name
            })

    pd.DataFrame(roster_rows).drop_duplicates().to_csv(ROSTERS_CSV, index=False)
    pd.DataFrame(player_rows).drop_duplicates().to_csv(PLAYERS_CSV, index=False)

    print(f"Wrote {ROSTERS_CSV} ({len(roster_rows)} rows)")
    print(f"Wrote {PLAYERS_CSV}")

    # ---------- STANDINGS ----------
    print("Fetching standings...")
    standings_data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")

    standings_rows = []
    teams_node = find_first(standings_data, "teams")
    teams = ensure_list(teams_node.get("team")) if isinstance(teams_node, dict) else []

    for t in teams:
        team_key = t.get("team_key")
        team_name = extract_team_name(t)
        ts = find_first(t, "team_standings") or {}

        standings_rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "rank": ts.get("rank"),
            "wins": ts.get("wins"),
            "losses": ts.get("losses"),
            "ties": ts.get("ties"),
            "pct": ts.get("percentage")
        })

    pd.DataFrame(standings_rows).to_csv(STANDINGS_CSV, index=False)
    print(f"Wrote {STANDINGS_CSV}")


if __name__ == "__main__":
    main()
