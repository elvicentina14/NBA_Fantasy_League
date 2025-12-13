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
        print("HTTP", r.status_code, r.text[:800])
        return None
    try:
        return r.json()
    except Exception as e:
        print("JSON decode err:", e)
        return None

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

def normalize_player_from_wrapper(wrapper):
    pk = None
    pid = None
    pname = None
    if isinstance(wrapper, dict):
        pk = wrapper.get("player_key") or wrapper.get("player",{}).get("player_key")
        pid = wrapper.get("player_id") or wrapper.get("player",{}).get("player_id")
        name = wrapper.get("name") or wrapper.get("player",{}).get("name")
        if isinstance(name, dict):
            pname = name.get("full") or (name.get("first")+" "+name.get("last") if name.get("first") and name.get("last") else None)
        else:
            pname = name
    elif isinstance(wrapper, list):
        for small in wrapper:
            if not isinstance(small, dict):
                continue
            if "player_key" in small and not pk:
                pk = small.get("player_key")
            if "player_id" in small and not pid:
                pid = small.get("player_id")
            if "name" in small and not pname:
                nm = small.get("name")
                if isinstance(nm, dict):
                    pname = nm.get("full") or (nm.get("first")+" "+nm.get("last") if nm.get("first") and nm.get("last") else None)
                else:
                    pname = nm
            if "editorial_player_key" in small and not pk:
                epk = small.get("editorial_player_key")
                if epk and '.' in epk:
                    # best-effort canonicalization: if league uses 466 as game key
                    if epk.startswith('nba.p.') and pid:
                        pk = f"466.p.{pid}"
                    else:
                        pk = epk
    if pk is None and pid:
        pk = pid
    return {"player_key": pk, "player_id": pid, "player_name": pname, "raw": wrapper}

# Get league metadata
meta_url = f"{ROOT}/league/{LEAGUE_KEY}/metadata?format=json"
meta = safe_get(meta_url)
current_week = find_key(meta, "current_week") if meta else None
current_date = find_key(meta, "current_date") if meta else None
print("League current_week:", current_week, "current_date:", current_date)

players = []
start = 0
count = 25
while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;status=ALL;start={start};count={count}?format=json"
    j = safe_get(url)
    if not j:
        print("Failed to get players page start", start)
        break

    players_container = find_key(j, "players") or find_key(j, "player")
    if players_container is None:
        print("No players container found on page start", start)
        break

    # numeric-key dict case
    if isinstance(players_container, dict):
        for k, v in players_container.items():
            if k == "count":
                continue
            node = None
            if isinstance(v, dict) and "player" in v:
                node = v["player"]
            elif isinstance(v, list) and v:
                node = v
            if node is None:
                continue
            if isinstance(node, list) and len(node) == 1 and isinstance(node[0], list):
                player_wrapper = node[0]
                rec = normalize_player_from_wrapper(player_wrapper)
                players.append(rec)
            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, list):
                        rec = normalize_player_from_wrapper(item)
                        players.append(rec)
                    elif isinstance(item, dict):
                        rec = normalize_player_from_wrapper(item)
                        players.append(rec)
            elif isinstance(node, dict):
                rec = normalize_player_from_wrapper(node)
                players.append(rec)
    elif isinstance(players_container, list):
        for entry in players_container:
            rec = normalize_player_from_wrapper(entry)
            players.append(rec)

    print("Fetched players page start", start, "-> total so far", len(players))
    if len(players) < start + count:
        break
    start += count
    time.sleep(0.25)

print("Total players collected:", len(players))

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

# Fetch per-player stats (date preferred)
stats_rows = []
stat_mode = "date" if current_date else ("week" if current_week else "season")
print("Fetching player stats using mode:", stat_mode)

for i,p in enumerate(players, start=1):
    pk = p.get("player_key") or p.get("player_id")
    if not pk:
        print("Skipping player, no key/id:", p)
        continue

    if stat_mode == "date":
        url = f"{ROOT}/player/{pk}/stats?format=json&date={current_date}"
    elif stat_mode == "week":
        url = f"{ROOT}/player/{pk}/stats;type=week;week={current_week}?format=json"
    else:
        url = f"{ROOT}/player/{pk}/stats?format=json"

    j = safe_get(url)
    if not j:
        stats_rows.append({"player_key": pk, "player_name": p.get("player_name"), "stat_id": None, "stat_value": None})
        time.sleep(0.12)
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
