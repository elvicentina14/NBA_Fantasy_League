# fetch_rosters_and_standings.py
import os, json, time, csv, sys
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    print("ERROR: set LEAGUE_KEY environment variable")
    sys.exit(2)

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def safe_get(url):
    r = session.get(url)
    print("GET", r.status_code, url)
    if r.status_code != 200:
        print("HTTP", r.status_code, r.text[:800])
        return None
    try:
        return r.json()
    except Exception as e:
        print("JSON decode err:", e)
        return None

# get teams listing
league_url = f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json"
j = safe_get(league_url)
if not j:
    print("Failed to fetch league teams")
    sys.exit(2)

# locate teams node (matches shape you shared earlier)
teams_node = None
try:
    teams_node = j["fantasy_content"]["league"][1]["teams"]
except Exception:
    # fallback search
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
    teams_node = find_key(j, "teams")

if teams_node is None:
    print("Could not find teams node")
    sys.exit(2)

count = int(teams_node.get("count", 0))
team_keys = []
for i in range(count):
    entry = teams_node.get(str(i))
    if not entry:
        continue
    try:
        # entry.team.[0].[0].team_key per your JSON shape
        team_key = entry["team"][0][0].get("team_key")
        team_keys.append(team_key)
    except Exception:
        continue

print("Found team keys:", team_keys)

# fetch rosters
rows = []
for tkey in team_keys:
    roster_url = f"{ROOT}/team/{tkey}/roster?format=json"
    r = safe_get(roster_url)
    if not r:
        continue
    try:
        team_block = r["fantasy_content"]["team"][0]
        team_key = team_block[0].get("team_key")
        team_name = team_block[2].get("name")
        players_block = r["fantasy_content"]["team"][1]["roster"]["0"]["players"]
    except Exception as e:
        print("Roster parse error for", tkey, e)
        continue

    # players_block is dict keyed by "0","1",... with each 'player' wrapper
    for k,v in players_block.items():
        try:
            player_wrapper = v["player"][0]  # list wrapper
            # wrapper is list of small dicts as before
            player_key = None
            player_name = None
            pos = None
            for item in player_wrapper:
                if not isinstance(item, dict):
                    continue
                if "player_key" in item and not player_key:
                    player_key = item.get("player_key")
                if "name" in item and not player_name:
                    nm = item.get("name")
                    if isinstance(nm, dict):
                        player_name = nm.get("full")
                    else:
                        player_name = nm
                if "display_position" in item and not pos:
                    pos = item.get("display_position")
            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": player_key,
                "player_name": player_name,
                "position": pos
            })
        except Exception as e:
            print("player entry parse fail", e)
    time.sleep(0.12)

# write team_rosters.csv
with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["team_key","team_name","player_key","player_name","position"])
    w.writeheader()
    for r in rows:
        w.writerow(r)
print("Wrote team_rosters.csv rows:", len(rows))

# Fetch standings
stand_url = f"{ROOT}/league/{LEAGUE_KEY}/standings?format=json"
s = safe_get(stand_url)
if not s:
    print("Failed to fetch standings")
else:
    try:
        stand_teams = s["fantasy_content"]["league"][1]["standings"]["0"]["teams"]
        scount = int(stand_teams.get("count",0))
        out = []
        for i in range(scount):
            t = stand_teams.get(str(i))["team"]
            tk = t[0].get("team_key")
            name = t[2].get("name")
            team_standings = t[3].get("team_standings", {})
            outcome_totals = team_standings.get("outcome_totals", {})
            out.append({
                "team_key": tk,
                "team_name": name,
                "wins": outcome_totals.get("wins"),
                "losses": outcome_totals.get("losses"),
                "ties": outcome_totals.get("ties"),
                "win_pct": outcome_totals.get("percentage"),
                "rank": team_standings.get("rank")
            })
        with open("standings.csv","w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["team_key","team_name","wins","losses","ties","win_pct","rank"])
            w.writeheader()
            for r in out:
                w.writerow(r)
        print("Wrote standings.csv rows:", len(out))
    except Exception as e:
        print("Standings parse error", e)
