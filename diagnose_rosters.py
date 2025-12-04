# diagnose_rosters.py
import os, json, time, sys
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    print("ERROR: LEAGUE_KEY not set")
    sys.exit(2)

oauth = OAuth2(None, None, from_file="oauth2.json")
def safe_get(url, params=None):
    r = oauth.session.get(url, params=params)
    print("GET", r.status_code, url, "params=", params)
    if r.status_code != 200:
        print("HTTP", r.status_code, r.text[:500])
        return None
    try:
        return r.json()
    except Exception as e:
        print("JSON decode err:", e)
        return None

# fetch league metadata (to get current_date/week)
league_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
league_resp = safe_get(league_url)
if not league_resp:
    print("Failed to fetch league teams")
    sys.exit(3)

# find league meta
def find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj: return obj[key]
        for v in obj.values():
            r = find_key(v, key)
            if r is not None: return r
    elif isinstance(obj, list):
        for e in obj:
            r = find_key(e, key)
            if r is not None: return r
    return None

league_node = find_key(league_resp, "league")
current_date = None
current_week = None
if isinstance(league_node, list):
    for el in league_node:
        if isinstance(el, dict) and "current_date" in el:
            current_date = el.get("current_date")
        if isinstance(el, dict) and "current_week" in el:
            current_week = el.get("current_week")
else:
    if isinstance(league_node, dict):
        current_date = league_node.get("current_date")
        current_week = league_node.get("current_week")

print("League current_date:", current_date, "current_week:", current_week)

# get teams wrappers
teams_node = find_key(league_resp, "teams")
team_wrappers = []
if isinstance(teams_node, dict):
    for k,v in teams_node.items():
        if k == "count": continue
        team_wrappers.append(v)
elif isinstance(teams_node, list):
    team_wrappers = teams_node

team_keys = []
team_ids = []
for w in team_wrappers:
    try:
        tlist = w.get("team")
        inner = tlist[0]
        tk = None; tid = None; tname = None
        for item in inner:
            if isinstance(item, dict):
                if "team_key" in item: tk = item["team_key"]
                if "team_id" in item: tid = item["team_id"]
                if "name" in item: tname = item["name"]
        if tk:
            team_keys.append((tk, tid, tname))
    except Exception:
        continue

print("Found", len(team_keys), "teams to inspect.")

def find_players_nodes(obj, out=None):
    # returns list of dicts or nodes that look like players (contain player_id or player_key)
    if out is None: out = []
    if isinstance(obj, dict):
        if "player" in obj:
            out.append(("key_player", obj["player"]))
        for k,v in obj.items():
            if isinstance(v, dict) and ("player_id" in v or "player_key" in v):
                out.append(("player_dict", v))
            find_players_nodes(v, out)
    elif isinstance(obj, list):
        for e in obj:
            find_players_nodes(e, out)
    return out

# fetch and dump raw per-team JSON, count 'player' occurrences
for tk, tid, tname in team_keys:
    print("---- Team:", tk, tid, tname)
    candidates = []
    if current_date:
        candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster", {"format":"json","date":current_date}))
    if current_week:
        candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster", {"format":"json","week":current_week}))
    candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster/players", {"format":"json"}))
    candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster", {"format":"json"}))

    data = None
    for url, params in candidates:
        data = safe_get(url, params=params)
        if data:
            break
        time.sleep(0.25)
    if not data:
        print("No data for team", tk)
        continue

    # save raw
    raw_filename = f"raw_team_{tid or tk}.json"
    with open(raw_filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(" Wrote", raw_filename)

    hits = find_players_nodes(data, out=[])
    print(" Found player-like nodes:", len(hits))
    # print first few hits types
    for i, h in enumerate(hits[:6]):
        t, node = h
        # print node summary
        if isinstance(node, list):
            print("  hit", i, "type", t, "list len", len(node))
        elif isinstance(node, dict):
            print("  hit", i, "type", t, "keys:", list(node.keys())[:10])
        else:
            print("  hit", i, "type", t, "type(node)=", type(node))
    time.sleep(0.2)

print("Diagnostic complete. Inspect raw_team_*.json files for details.")
