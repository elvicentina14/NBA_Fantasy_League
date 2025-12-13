# fetch_all_rosters.py (robust roster + player extraction with date/week support)
import os, json, time, csv, sys
from yahoo_oauth import OAuth2

PROJECT_DIR = os.getcwd()
OUT_JSON = os.path.join(PROJECT_DIR, "players_long.json")
OUT_CSV = os.path.join(PROJECT_DIR, "players_long.csv")

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    print("ERROR: LEAGUE_KEY not set")
    sys.exit(2)

oauth = OAuth2(None, None, from_file="oauth2.json")
if not oauth.token_is_valid():
    print("Warning: oauth token not valid; library will attempt refresh.")

def safe_get(url, params=None):
    r = oauth.session.get(url, params=params)
    print("GET", r.status_code, url, "params=", params)
    if r.status_code != 200:
        print("Response start:", r.text[:800])
        return None
    try:
        return r.json()
    except Exception as e:
        print("JSON decode error:", e)
        return None

# 1) fetch teams + league metadata (to get current_date/week)
league_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
league_resp = safe_get(league_url)
if not league_resp:
    print("Failed to fetch league teams; aborting.")
    sys.exit(3)

# find fantasy_content -> league element that contains metadata and teams
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

# get league element (list with metadata and teams)
fc = league_resp.get("fantasy_content") or league_resp
league_node = find_key(fc, "league")
# league_node may be a list (we saw a [metadata, teams] pattern)
if isinstance(league_node, list):
    # pick first element with 'num_teams' as metadata, second with 'teams' might be teams
    league_meta = None
    league_with_teams = None
    for el in league_node:
        if isinstance(el, dict) and "num_teams" in el:
            league_meta = el
        if isinstance(el, dict) and "teams" in el:
            league_with_teams = el
else:
    league_meta = league_node
    league_with_teams = league_node

current_date = None
current_week = None
if isinstance(league_meta, dict):
    current_date = league_meta.get("current_date") or league_meta.get("current_date")
    current_week = league_meta.get("current_week") or league_meta.get("current_week")

print("League current_date:", current_date, "current_week:", current_week)

teams_node = None
if isinstance(league_with_teams, dict):
    teams_node = league_with_teams.get("teams") or league_with_teams.get("teams")
if teams_node is None:
    # fallback: try find teams anywhere
    teams_node = find_key(league_resp, "teams")

if not teams_node:
    print("Could not locate teams node. Aborting.")
    sys.exit(4)

# normalize teams wrappers
team_wrappers = []
if isinstance(teams_node, dict):
    for k,v in teams_node.items():
        if k == "count": continue
        team_wrappers.append(v)
elif isinstance(teams_node, list):
    team_wrappers = teams_node

team_keys = []
team_meta = {}
for wrapper in team_wrappers:
    try:
        tlist = wrapper.get("team")
        if isinstance(tlist, list) and len(tlist) > 0:
            inner = tlist[0]
            tk = None; tid = None; tname = None
            for item in inner:
                if isinstance(item, dict):
                    if "team_key" in item: tk = item["team_key"]
                    if "team_id" in item: tid = item["team_id"]
                    if "name" in item: tname = item["name"]
            if tk:
                team_keys.append(tk)
                team_meta[tk] = {"team_id": tid, "team_name": tname}
    except Exception:
        continue

print("Found", len(team_keys), "team keys.")

# robust player finder: recurse and collect dicts that look like players
def find_players(obj, out=None):
    if out is None: out = []
    if isinstance(obj, dict):
        # If this dict looks like a player record (has player_id or player_key), collect it
        if "player_id" in obj or "player_key" in obj:
            out.append(obj)
            return out
        # Sometimes players are represented as {'player': { ... }} or {'player': [ ... ]}
        if "player" in obj:
            pv = obj["player"]
            if isinstance(pv, list):
                for e in pv:
                    find_players(e, out)
                return out
            elif isinstance(pv, dict):
                find_players(pv, out)
                return out
        # check nested fields
        for v in obj.values():
            find_players(v, out)
    elif isinstance(obj, list):
        # handle list of small dicts that together form a player (Yahoo sometimes returns player as [ {id},{name},{pos},... ])
        for item in obj:
            find_players(item, out)
    return out

# helper: merge list-of-dict wrappers into a single player dict (if needed)
def normalize_player(wrapper):
    # wrapper might be dict already (good)
    if isinstance(wrapper, dict):
        return wrapper
    # if wrapper is a list of small dicts, merge into one dict
    if isinstance(wrapper, list):
        merged = {}
        for elem in wrapper:
            if isinstance(elem, dict):
                for k,v in elem.items():
                    # if same key appears multiple times keep last
                    merged[k] = v
        return merged
    return wrapper

all_players = []
rows = []

for i, tk in enumerate(team_keys, start=1):
    print(f"Fetching roster for {tk} ({i}/{len(team_keys)})")
    # try endpoints, preferring date/week parameters for date-scoped leagues
    tried = []
    candidates = []
    if current_date:
        candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster", {"format":"json","date":current_date}))
    if current_week:
        candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster", {"format":"json","week":current_week}))
    # fallback endpoints
    candidates.append((f"https://fantasys.yahoo.com/fantasy/v2/team/{tk}/roster", {"format":"json"}))
    candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster/players", {"format":"json"}))
    candidates.append((f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster", {"format":"json"}))

    data = None
    for url, params in candidates:
        tried.append((url, params))
        data = safe_get(url, params=params)
        if data:
            break
        time.sleep(0.25)
    if not data:
        print("No roster data for", tk, "tried:", tried[:3])
        continue

    # find players in returned structure
    players_found = find_players(data)
    # normalize player wrappers into usable dicts
    normalized = []
    for p in players_found:
        if isinstance(p, dict):
            normalized.append(p)
        else:
            normalized.append(normalize_player(p))

    print(" raw player nodes found:", len(players_found), "normalized:", len(normalized))

    for p in normalized:
        # try to extract key fields robustly
        player_id = p.get("player_id") or p.get("player", {}).get("player_id") if isinstance(p.get("player"), dict) else None
        player_key = p.get("player_key") or p.get("player", {}).get("player_key") if isinstance(p.get("player"), dict) else None
        name_field = p.get("name") or (p.get("player", {}).get("name") if isinstance(p.get("player"), dict) else None)
        # name may be dict like {'full': 'First Last'}
        if isinstance(name_field, dict):
            pname = name_field.get("full") or name_field.get("name")
        else:
            pname = name_field
        selected_position = p.get("selected_position") or (p.get("position") if isinstance(p.get("position"), str) else None)

        # collect player record
        rec = {
            "team_key": tk,
            "team_id": team_meta.get(tk, {}).get("team_id"),
            "team_name": team_meta.get(tk, {}).get("team_name"),
            "player_id": str(player_id) if player_id is not None else (str(player_key) if player_key is not None else None),
            "player_key": player_key,
            "player_name": pname,
            "selected_position": selected_position,
            "player_raw": p
        }
        all_players.append(rec)

        # If player has stats embedded, extract them (if present)
        statlist = None
        ps = p.get("player_stats") or p.get("stats") or None
        if isinstance(ps, dict):
            sc = ps.get("stats") or ps
            if isinstance(sc, dict):
                statlist = sc.get("stat")
        if not statlist and "stats" in p and isinstance(p.get("stats"), dict):
            sc = p.get("stats")
            statlist = sc.get("stat")
        if statlist and isinstance(statlist, list):
            for s in statlist:
                sid = s.get("stat_id") if isinstance(s, dict) else None
                sval = None
                if isinstance(s, dict):
                    v = s.get("value")
                    if isinstance(v, dict):
                        sval = v.get("value") or v.get("display_value") or None
                    else:
                        sval = v
                rows.append({
                    "team_key": tk,
                    "team_id": team_meta.get(tk, {}).get("team_id"),
                    "team_name": team_meta.get(tk, {}).get("team_name"),
                    "player_id": rec["player_id"],
                    "player_name": pname,
                    "selected_position": selected_position,
                    "stat_id": sid,
                    "stat_value": sval
                })
        else:
            # still add a row for rostered player (null stats)
            rows.append({
                "team_key": tk,
                "team_id": team_meta.get(tk, {}).get("team_id"),
                "team_name": team_meta.get(tk, {}).get("team_name"),
                "player_id": rec["player_id"],
                "player_name": pname,
                "selected_position": selected_position,
                "stat_id": None,
                "stat_value": None
            })

    # polite wait
    time.sleep(0.35)

# save outputs
with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(all_players, f, indent=2, ensure_ascii=False)
print("Saved combined player JSON:", OUT_JSON)

# write CSV (flattened)
fieldnames = ["team_key","team_id","team_name","player_id","player_name","selected_position","stat_id","stat_value"]
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
print("Saved flattened CSV:", OUT_CSV, "rows:", len(rows))
