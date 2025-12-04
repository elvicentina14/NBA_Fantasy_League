# fetch_players_and_stats.py
import os, json, time, csv, sys
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    print("ERROR: set LEAGUE_KEY environment variable (eg: nba.l.165651)")
    sys.exit(2)

oauth = OAuth2(None, None, from_file="oauth2.json")
if not oauth.token_is_valid():
    print("Warning: oauth token not valid; library will try refresh.")

ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def safe_get(url):
    r = oauth.session.get(url)
    print("GET", r.status_code, url)
    if r.status_code != 200:
        print("HTTP", r.status_code, r.text[:600])
        return None
    try:
        return r.json()
    except Exception as e:
        print("JSON decode err:", e)
        return None

# helper to find keys nested in JSON
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

# 1) get league metadata
meta_url = f"{ROOT}/league/{LEAGUE_KEY}/metadata?format=json"
meta = safe_get(meta_url)
current_week = None
current_date = None
if meta:
    current_week = find_key(meta, "current_week")
    current_date = find_key(meta, "current_date")
print("League current_week:", current_week, "current_date:", current_date)

# 2) page through league players (status=T -> taken/on teams)
players = []
start = 0
count = 25
while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;status=T;start={start};count={count}?format=json"
    j = safe_get(url)
    if not j:
        print("Failed to get players page start", start)
        break

    player_nodes = find_key(j, "player")
    if not player_nodes:
        print("No 'player' node found at start", start)
        break

    if isinstance(player_nodes, dict):
        player_nodes = [player_nodes]

    page_count = 0
    for p in player_nodes:
        player_key = find_key(p, "player_key")
        player_id = find_key(p, "player_id")
        name = find_key(p, "full") or find_key(p, "name") or find_key(p, "nickname") or find_key(p, "editorial_player_key")
        if isinstance(name, dict):
            name = name.get("full") or name.get("first") and (name.get("first")+" "+name.get("last")) or str(name)
        players.append({"player_key": player_key, "player_id": player_id, "player_name": name, "raw": p})
        page_count += 1

    print("Fetched", page_count, "players (start", start, ")")
    if page_count < count:
        break
    start += count
    time.sleep(0.35)

print("Total players collected:", len(players))

# Save master players CSV
with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["player_key","player_id","player_name"])
    writer.writeheader()
    for p in players:
        writer.writerow({
            "player_key": p.get("player_key"),
            "player_id": p.get("player_id"),
            "player_name": p.get("player_name")
        })
print("Wrote league_players.csv")

# 3) fetch per-player stats
stats_rows = []
if current_week:
    stat_mode = "week"
elif current_date:
    stat_mode = "date"
else:
    stat_mode = "season"

print("Fetching player stats using mode:", stat_mode)

for i, p in enumerate(players, start=1):
    pk = p.get("player_key") or p.get("player_id")
    if not pk:
        print("Skipping player, no key/id:", p)
        continue

    if stat_mode == "week":
        url = f"{ROOT}/player/{pk}/stats;type=week;week={current_week}?format=json"
    elif stat_mode == "date":
        url = f"{ROOT}/player/{pk}/stats?format=json&date={current_date}"
    else:
        url = f"{ROOT}/player/{pk}/stats?format=json"

    j = safe_get(url)
    if not j:
        print("No stats for player", pk)
        time.sleep(0.15)
        continue

    statlist = find_key(j, "stat")
    if statlist is None:
        stats_rows.append({"player_key": pk, "player_name": p.get("player_name"), "stat_id": None, "stat_value": None})
    else:
        if isinstance(statlist, dict):
            statlist = [statlist]
        for s in statlist:
            sid = find_key(s, "stat_id") or s.get("stat_id")
            sval = find_key(s, "value") or s.get("value") or find_key(s, "display_value")
            if isinstance(sval, dict):
                sval = sval.get("value") or sval.get("display_value")
            stats_rows.append({"player_key": pk, "player_name": p.get("player_name"), "stat_id": sid, "stat_value": sval})

    if i % 10 == 0:
        time.sleep(0.6)
    else:
        time.sleep(0.15)

with open("player_stats.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["player_key","player_name","stat_id","stat_value"])
    w.writeheader()
    for r in stats_rows:
        w.writerow(r)
print("Wrote player_stats.csv rows:", len(stats_rows))
