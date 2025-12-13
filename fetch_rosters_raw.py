from yahoo_oauth import OAuth2
import os
import csv

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def find_all(obj, key):
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                out.append(v)
            out.extend(find_all(v, key))
    elif isinstance(obj, list):
        for i in obj:
            out.extend(find_all(i, key))
    return out

def get_json(oauth, path):
    url = f"{BASE}/{path}?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY not set")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)

    print("Fetching teams...")
    league = get_json(oauth, f"league/{LEAGUE_KEY}/teams")
    teams = []

    for t in find_all(league, "team"):
        if isinstance(t, dict):
            k = t.get("team_key")
            n = t.get("name")
            if k and n:
                teams.append((k, n))

    teams = list(dict.fromkeys(teams))
    print(f"Found {len(teams)} teams")

    rows = []

    for idx, (team_key, team_name) in enumerate(teams, 1):
        print(f"[{idx}/{len(teams)}] {team_name}")
        roster = get_json(oauth, f"team/{team_key}/roster")

        for p in find_all(roster, "player"):
            if not isinstance(p, dict):
                continue

            pk = p.get("player_key")
            name = (p.get("name") or {}).get("full")

            positions = [
                x for x in find_all(p, "position") if isinstance(x, str)
            ]

            if pk and name:
                rows.append({
                    "team_key": team_key,
                    "team_name": team_name,
                    "player_key": pk,
                    "player_name": name,
                    "positions": ",".join(positions)
                })

    with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["team_key", "team_name", "player_key", "player_name", "positions"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows → team_rosters.csv")

if __name__ == "__main__":
    main()
