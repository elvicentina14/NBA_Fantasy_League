import os, json, time, csv, sys
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    print("ERROR: set LEAGUE_KEY environment variable")
    sys.exit(2)

oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def safe_get(url):
    r = oauth.session.get(url)
    print("GET", r.status_code, url)
    if r.status_code != 200:
        print("HTTP", r.status_code, r.text[:500])
        return None
    return r.json()

def find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for e in obj:
            r = find_key(e, key)
            if r is not None:
                return r
    return None

players = []
start = 0
count = 25

while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;status=ALL;start={start};count={count}?format=json"
    j = safe_get(url)
    if not j:
        break

    players_node = find_key(j, "players")
    if not players_node:
        break

    for k,v in players_node.items():
        if k == "count":
            continue
        wrapper = v.get("player") if isinstance(v, dict) else None
        if not wrapper:
            continue

        player_key = None
        player_id = None
        editorial_key = None
        name = None

        for item in wrapper:
            if not isinstance(item, dict):
                continue
            player_key = player_key or item.get("player_key")
            player_id = player_id or item.get("player_id")
            editorial_key = editorial_key or item.get("editorial_player_key")
            if "name" in item:
                nm = item["name"]
                if isinstance(nm, dict):
                    name = nm.get("full")
                else:
                    name = nm

        players.append({
            "player_key": player_key,
            "player_id": player_id,
            "editorial_player_key": editorial_key,
            "player_name": name
        })

    if len(players) < start + count:
        break
    start += count
    time.sleep(0.3)

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=["player_key","player_id","editorial_player_key","player_name"]
    )
    w.writeheader()
    for p in players:
        w.writerow(p)

print("Wrote league_players.csv rows:", len(players))
