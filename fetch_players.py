# fetch_players.py
import os, csv, time, sys
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    sys.exit("LEAGUE_KEY not set")

oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

def find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            x = find(v, key)
            if x is not None:
                return x
    elif isinstance(obj, list):
        for i in obj:
            x = find(i, key)
            if x is not None:
                return x
    return None

players = []
start = 0
count = 25

while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;status=ALL;start={start};count={count}?format=json"
    j = get(url)
    block = find(j, "players")
    if not block:
        break

    for k, v in block.items():
        if k == "count":
            continue
        wrapper = v.get("player", [])
        pkey = find(wrapper, "player_key")
        pid = find(wrapper, "player_id")
        name = find(wrapper, "full")

        if pkey:
            players.append({
                "player_key": pkey,
                "player_id": pid,
                "player_name": name
            })

    if len(players) < start + count:
        break
    start += count
    time.sleep(0.2)

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["player_key","player_id","player_name"])
    w.writeheader()
    w.writerows(players)

print("Wrote league_players.csv rows:", len(players))
