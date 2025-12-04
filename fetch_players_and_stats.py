# fetch_players_and_stats_debug.py
# Debugging/fallback version to discover why only 1 player is found.
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

# discover current_week/current_date
meta = safe_get(f"{ROOT}/league/{LEAGUE_KEY}/metadata?format=json")
current_week = find_key(meta, "current_week") if meta else None
current_date = find_key(meta, "current_date") if meta else None
print("League metadata -> current_week:", current_week, "current_date:", current_date)

# Try multiple status filters and page through results to collect players.
statuses_to_try = ["T", "ALL", "A", "O", "Y"]  # T: taken, ALL: all, A: active - we try several
collected = {}
for status in statuses_to_try:
    players = []
    start = 0
    count = 25
    first_page_dumped = False
    while True:
        url = f"{ROOT}/league/{LEAGUE_KEY}/players;status={status};start={start};count={count}?format=json"
        j = safe_get(url)
        if not j:
            print(f"No JSON returned for status={status} start={start}")
            break

        # dump first page for inspection
        if not first_page_dumped:
            fname = f"debug_players_{status}_page{start}.json"
            try:
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(j, f, indent=2, ensure_ascii=False)
                print("Wrote debug file:", fname)
            except Exception as e:
                print("Failed writing debug file:", e)
            first_page_dumped = True

        # Find player nodes robustly
        player_nodes = find_key(j, "player")
        # Some responses wrap players in 'players' -> '0' -> 'player' etc. If no direct 'player' found, try other heuristics:
        if player_nodes is None:
            # try scanning for lists/dicts that contain 'player_key' or 'player_id'
            def scan_for_player_like(o, out=None):
                if out is None: out=[]
                if isinstance(o, dict):
                    if "player_key" in o or "player_id" in o:
                        out.append(o)
                    for v in o.values():
                        scan_for_player_like(v, out)
                elif isinstance(o, list):
                    for e in o:
                        scan_for_player_like(e, out)
                return out
            guessed = scan_for_player_like(j, [])
            if guessed:
                player_nodes = guessed
                print(f"status={status} start={start} found {len(guessed)} guessed player-like nodes")
        # normalize
        if player_nodes is None:
            print(f"status={status} start={start} -> no player nodes found")
            break

        # ensure list
        if isinstance(player_nodes, dict):
            player_nodes = [player_nodes]

        page_count = 0
        for p in player_nodes:
            # robust extraction
            def gk(o, k):
                v = find_key(o, k)
                return v
            player_key = gk(p, "player_key") or gk(p, "editorial_player_key") or gk(p, "player_id")
            player_id = gk(p, "player_id")
            name = gk(p, "full") or gk(p, "name") or gk(p, "editorial_name") or gk(p, "editorial_player_key")
            if isinstance(name, dict):
                name = name.get("full") or str(name)
            players.append({"player_key": player_key, "player_id": player_id, "player_name": name, "raw": p})
            page_count += 1

        print(f"status={status} start={start} fetched {page_count} players")
        if page_count < count:
            break
        start += count
        time.sleep(0.25)

    print(f"Status {status}: total players found = {len(players)}")
    collected[status] = players
    # if we found >1 player for this status, we can stop trying other statuses (but we still dumped first pages for diagnosis)
    if len(players) > 1:
        print("Looks good — got more than 1 player for status", status)
        break

# Choose the best (status with most players)
best_status = max(collected.keys(), key=lambda s: len(collected[s]))
players = collected.get(best_status, [])
print("Best status chosen:", best_status, "player count:", len(players))

# Save full raw players JSON for the chosen status
try:
    with open("league_players_raw.json", "w", encoding="utf-8") as f:
        json.dump([p.get("raw") for p in players], f, indent=2, ensure_ascii=False)
    print("Wrote league_players_raw.json")
except Exception as e:
    print("Failed write league_players_raw.json:", e)

# Write normalized CSV of players
with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["player_key","player_id","player_name"])
    w.writeheader()
    for p in players:
        w.writerow({
            "player_key": p.get("player_key"),
            "player_id": p.get("player_id"),
            "player_name": p.get("player_name")
        })
print("Wrote league_players.csv rows:", len(players))

# If we only have 1 or 0 players, print a hint for manual inspection
if len(players) <= 1:
    print("WARNING: only", len(players), "players found. Inspect the debug_players_*.json files to see the raw shape returned by Yahoo.")

# Now fetch stats for each discovered player (if any)
stats_rows = []
if players:
    # best effort for stat mode
    stat_mode = "week" if find_key(meta if meta else {}, "current_week") else ("date" if find_key(meta if meta else {}, "current_date") else "season")
    current_week = find_key(meta, "current_week") if meta else None
    current_date = find_key(meta, "current_date") if meta else None
    print("Fetching stats using mode:", stat_mode, "week:", current_week, "date:", current_date)
    for i,p in enumerate(players, start=1):
        pk = p.get("player_key") or p.get("player_id")
        if not pk:
            print("Skipping player missing key:", p)
            continue
        if stat_mode == "week":
            url = f"{ROOT}/player/{pk}/stats;type=week;week={current_week}?format=json"
        elif stat_mode == "date":
            url = f"{ROOT}/player/{pk}/stats?format=json&date={current_date}"
        else:
            url = f"{ROOT}/player/{pk}/stats?format=json"
        j = safe_get(url)
        if not j:
            print("No stats JSON for", pk)
            stats_rows.append({"player_key": pk, "player_name": p.get("player_name"), "stat_id": None, "stat_value": None})
            continue
        # dump first player's stat json for inspection
        if i == 1:
            try:
                with open("debug_playerstats_first.json", "w", encoding="utf-8") as f:
                    json.dump(j, f, indent=2, ensure_ascii=False)
                print("Wrote debug_playerstats_first.json")
            except:
                pass

        statlist = find_key(j, "stat")
        if not statlist:
            stats_rows.append({"player_key": pk, "player_name": p.get("player_name"), "stat_id": None, "stat_value": None})
            continue
        if isinstance(statlist, dict):
            statlist = [statlist]
        for s in statlist:
            sid = find_key(s, "stat_id") or s.get("stat_id")
            sval = find_key(s, "value") or s.get("value") or find_key(s, "display_value")
            if isinstance(sval, dict):
                sval = sval.get("value") or sval.get("display_value")
            stats_rows.append({"player_key": pk, "player_name": p.get("player_name"), "stat_id": sid, "stat_value": sval})
        time.sleep(0.12)

with open("player_stats.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["player_key","player_name","stat_id","stat_value"])
    w.writeheader()
    for r in stats_rows:
        w.writerow(r)
print("Wrote player_stats.csv rows:", len(stats_rows))
