# fetch_all_rosters.py
# Fetch teams -> rosters -> player gamelogs and write players_long.json and players_long.csv
import os, json, time, csv, sys
from yahoo_oauth import OAuth2

PROJECT_DIR = os.getcwd()
OUT_JSON = os.path.join(PROJECT_DIR, "players_long.json")
OUT_CSV = os.path.join(PROJECT_DIR, "players_long.csv")

# env config
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")  # injected by Actions from secret
if not LEAGUE_KEY:
    print("ERROR: LEAGUE_KEY env var missing")
    sys.exit(2)

# oauth2.json file is written by workflow from the secret; library will refresh tokens itself
oauth = OAuth2(None, None, from_file="oauth2.json")
if not oauth.token_is_valid():
    print("Warning: token not valid — trying refresh (the library will attempt it).")

def safe_get(url, params=None):
    r = oauth.session.get(url, params=params)
    print("GET", r.status_code, url, "params=", params)
    if r.status_code != 200:
        print("Response:", r.text[:1000])
        return None
    try:
        return r.json()
    except Exception as e:
        print("JSON decode error:", e)
        return None

# 1) fetch teams via API so we don't rely on a local teams file
teams_endpoint = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams"
resp = safe_get(teams_endpoint, params={"format": "json"})
if not resp:
    print("Failed to fetch league teams. Aborting.")
    sys.exit(3)

# locate teams node (robust)
def find_teams_node(obj):
    if isinstance(obj, dict):
        if "teams" in obj and isinstance(obj["teams"], (dict, list)):
            return obj["teams"]
        for v in obj.values():
            found = find_teams_node(v)
            if found:
                return found
    elif isinstance(obj, list):
        for e in obj:
            found = find_teams_node(e)
            if found:
                return found
    return None

teams_node = find_teams_node(resp)
if teams_node is None:
    print("Could not locate teams node in API response.")
    sys.exit(4)

# normalize teams into wrapper list
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

print("Found team keys:", len(team_keys))

all_players = []
rows = []

for i, tk in enumerate(team_keys, start=1):
    print(f"Fetching roster for {tk} ({i}/{len(team_keys)})")
    # two candidate endpoints for roster
    urls = [
        f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster?format=json",
        f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tk}/roster/players?format=json"
    ]
    data = None
    for url in urls:
        data = safe_get(url)
        if data:
            break
        time.sleep(0.25)
    if not data:
        print("No roster data for", tk)
        continue

    # find players in response
    def find_players(obj):
        if isinstance(obj, dict):
            if "player" in obj and isinstance(obj["player"], list):
                return obj["player"]
            for v in obj.values():
                r = find_players(v)
                if r:
                    return r
        elif isinstance(obj, list):
            for e in obj:
                r = find_players(e)
                if r:
                    return r
        return None

    players = find_players(data) or []
    print(" players found:", len(players))
    for p in players:
        rec = {
            "team_key": tk,
            "team_id": team_meta.get(tk, {}).get("team_id"),
            "team_name": team_meta.get(tk, {}).get("team_name"),
            "player_raw": p
        }
        all_players.append(rec)

        # extract basic fields if present
        player_id = None
        pname = None
        pos = None
        if isinstance(p, dict):
            player_id = p.get("player_id") or (p.get("player", {}).get("player_id") if isinstance(p.get("player"), dict) else None)
            name = p.get("name")
            if isinstance(name, dict):
                pname = name.get("full") or name.get("name")
            else:
                pname = name
            pos = p.get("selected_position") or p.get("position")

        # extract stats list if any
        statlist = None
        if isinstance(p, dict):
            ps = p.get("player_stats") or p.get("player_stats")
            if isinstance(ps, dict):
                stats_container = ps.get("stats")
                if isinstance(stats_container, dict):
                    statlist = stats_container.get("stat")
            if statlist is None and "stats" in p and isinstance(p.get("stats"), dict):
                statlist = p.get("stats").get("stat")

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
                    "player_id": str(player_id) if player_id is not None else None,
                    "player_name": pname,
                    "selected_position": pos,
                    "stat_id": sid,
                    "stat_value": sval
                })
        else:
            rows.append({
                "team_key": tk,
                "team_id": team_meta.get(tk, {}).get("team_id"),
                "team_name": team_meta.get(tk, {}).get("team_name"),
                "player_id": str(player_id) if player_id is not None else None,
                "player_name": pname,
                "selected_position": pos,
                "stat_id": None,
                "stat_value": None
            })

    time.sleep(0.35)

# save results
with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(all_players, f, indent=2, ensure_ascii=False)
print("Saved", OUT_JSON)

# write CSV
fieldnames = ["team_key","team_id","team_name","player_id","player_name","selected_position","stat_id","stat_value"]
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
print("Saved", OUT_CSV, "rows:", len(rows))
