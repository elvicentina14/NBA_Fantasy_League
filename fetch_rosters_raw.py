# fetch_rosters_raw.py
from yahoo_oauth import OAuth2
import os
import csv
import requests

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"

# ---------- helpers ----------

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def find_all(obj, key):
    """Recursively find ALL values for a given key."""
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                found.append(v)
            found.extend(find_all(v, key))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(find_all(item, key))
    return found

def get_json(oauth, path):
    url = f"{BASE_URL}/{path}"
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url += f"{sep}format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

# ---------- main logic ----------

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var not set")

    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("oauth2.json not found")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid – refresh locally")

    print("Fetching league teams (RAW API)...")
    league_data = get_json(oauth, f"league/{LEAGUE_KEY}/teams")

    # Extract team objects
    team_nodes = find_all(league_data, "team")

    teams = []
    for t in team_nodes:
        if not isinstance(t, dict):
            continue
        team_key = t.get("team_key")
        team_name = t.get("name")
        if team_key and team_name:
            teams.append((team_key, team_name))

    teams = list(dict.fromkeys(teams))  # de-dupe
    print(f"Found {len(teams)} teams")

    rows = []

    for idx, (team_key, team_name) in enumerate(teams, 1):
        print(f"[{idx}/{len(teams)}] Fetching roster for {team_name}")
        roster_data = get_json(oauth, f"team/{team_key}/roster")

        player_nodes = find_all(roster_data, "player")

        for p in player_nodes:
            if not isinstance(p, dict):
                continue

            player_key = p.get("player_key")
            name_obj = p.get("name") or {}
            player_name = name_obj.get("full")

            positions = find_all(p, "position")
            pos_str = ",".join([str(x) for x in positions if isinstance(x, str)])

            if player_key and player_name:
                rows.append({
                    "team_key": team_key,
                    "team_name": team_name,
                    "player_key": player_key,
                    "player_name": player_name,
                    "position": pos_str
                })

    print(f"Writing {len(rows)} rows to team_rosters.csv")

    with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["team_key", "team_name", "player_key", "player_name", "position"]
        )
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
