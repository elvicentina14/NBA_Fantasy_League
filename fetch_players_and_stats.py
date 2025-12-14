import os, sys, csv, time
from yahoo_oauth import OAuth2
from yahoo_helpers import flatten_list, extract_name

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    sys.exit("ERROR: LEAGUE_KEY not set")

oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"


def get(url):
    r = oauth.session.get(url)
    print("GET", r.status_code, url)
    if r.status_code != 200:
        return None
    return r.json()


def find(node, key):
    if isinstance(node, dict):
        if key in node:
            return node[key]
        for v in node.values():
            r = find(v, key)
            if r is not None:
                return r
    elif isinstance(node, list):
        for i in node:
            r = find(i, key)
            if r is not None:
                return r
    return None


players = []
start = 0
count = 25

while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;status=ALL;start={start};count={count}?format=json"
    j = get(url)
    if not j:
        break

    players_node = find(j, "players")
    if not isinstance(players_node, dict):
        break

    for k, entry in players_node.items():
        if k == "count":
            continue
        wrapper = entry.get("player") if isinstance(entry, dict) else None
        if not wrapper:
            continue

        wrapper = flatten_list(wrapper)

        pk = pid = epk = name = None
        for item in wrapper:
            if not isinstance(item, dict):
                continue
            pk = pk or item.get("player_key")
            pid = pid or item.get("player_id")
            epk = epk or item.get("editorial_player_key")
            name = name or extract_name(item)

        players.append({
            "player_key": pk,
            "player_id": pid,
            "editorial_player_key": epk,
            "player_name": name
        })

    if len(players) < start + count:
        break

    start += count
    time.sleep(0.25)

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=["player_key", "player_id", "editorial_player_key", "player_name"]
    )
    w.writehe
