from yahoo_oauth import OAuth2
import os
import pandas as pd

CONFIG_FILE = "oauth2.json"
BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def find_first(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find_first(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for i in obj:
            r = find_first(i, key)
            if r is not None:
                return r
    return None

def get_json(oauth, path):
    url = f"{BASE}/{path}?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

def main():
    if not os.path.exists("team_rosters.csv"):
        raise SystemExit("team_rosters.csv missing")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    rosters = pd.read_csv("team_rosters.csv", dtype=str)

    rows = []
    players = rosters["player_key"].dropna().unique().tolist()

    print(f"Fetching season totals for {len(players)} players")

    for i, pk in enumerate(players, 1):
        print(f"[{i}/{len(players)}] {pk}")
        try:
            data = get_json(oauth, f"player/{pk}/stats;type=season")
        except Exception as e:
            print("Failed:", pk, e)
            continue

        stats = find_first(data, "stats")
        if not isinstance(stats, dict):
            continue

        for s in ensure_list(stats.get("stat")):
            sid = s.get("stat_id")
            val = s.get("value")
            if sid is not None:
                rows.append({
                    "player_key": pk,
                    "stat_id": str(sid),
                    "stat_value": val
                })

    df = pd.DataFrame(rows)
    df.to_csv("player_season_totals.csv", index=False)
    print(f"✅ Wrote {len(df)} rows → player_season_totals.csv")

if __name__ == "__main__":
    main()
