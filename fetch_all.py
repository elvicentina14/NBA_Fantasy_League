# fetch_all.py
import os, sys, time, csv
from yahoo_oauth import OAuth2
import pandas as pd

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    print("ERROR: LEAGUE_KEY env var not set")
    sys.exit(2)

ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

def safe_get(url):
    r = session.get(url)
    print("GET", r.status_code, url)
    if r.status_code != 200:
        print("HTTP", r.status_code, r.text[:500])
        return None
    try:
        return r.json()
    except Exception as e:
        print("JSON decode error:", e)
        return None

def find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            out = find_key(v, key)
            if out is not None:
                return out
    elif isinstance(obj, list):
        for v in obj:
            out = find_key(v, key)
            if out is not None:
                return out
    return None

# ---------- A. LEAGUE METADATA ----------
meta_url = f"{ROOT}/league/{LEAGUE_KEY}/metadata?format=json"
meta = safe_get(meta_url)
current_week = find_key(meta, "current_week") if meta else None
current_date = find_key(meta, "current_date") if meta else None
print("current_week:", current_week, "current_date:", current_date)

# ---------- B. LEAGUE PLAYERS ----------
players = []
start = 0
page_size = 25

def normalize_player(wrapper):
    pk = None
    pid = None
    name = None

    if isinstance(wrapper, dict):
        pk = wrapper.get("player_key")
        pid = wrapper.get("player_id")
        nm = wrapper.get("name")
        if isinstance(nm, dict):
            name = nm.get("full") or f"{nm.get('first','')} {nm.get('last','')}".strip()
        else:
            name = nm
    elif isinstance(wrapper, list):
        for item in wrapper:
            if not isinstance(item, dict):
                continue
            if "player_key" in item and not pk:
                pk = item["player_key"]
            if "player_id" in item and not pid:
                pid = item["player_id"]
            if "name" in item and not name:
                nm = item["name"]
                if isinstance(nm, dict):
                    name = nm.get("full") or f"{nm.get('first','')} {nm.get('last','')}".strip()
                else:
                    name = nm

    if pk is None and pid:
        pk = pid
    return {
        "player_key": pk,
        "player_id": pid,
        "player_name": name,
    }

while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;status=ALL;start={start};count={page_size}?format=json"
    j = safe_get(url)
    if not j:
        break

    players_node = find_key(j, "players")
    if not players_node:
        print("No players node on page", start)
        break

    if isinstance(players_node, dict):
        count = int(players_node.get("count", 0))
        for i in range(count):
            entry = players_node.get(str(i))
            if not entry:
                continue
            node = entry.get("player")
            if not node:
                continue
            # shape: [ [ {...},{...},... ] ]
            if isinstance(node, list) and len(node) == 1 and isinstance(node[0], list):
                wrapper = node[0]
            else:
                wrapper = node
            players.append(normalize_player(wrapper))
    else:
        # unexpected shape, but try treating as list
        for entry in players_node:
            players.append(normalize_player(entry))

    print("Players total so far:", len(players))
    if len(players_node) < page_size:
        break
    start += page_size
    time.sleep(0.2)

# write league_players.csv
with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["player_key","player_id","player_name"])
    w.writeheader()
    for p in players:
        w.writerow(p)
print("Wrote league_players.csv with", len(players), "players")

# ---------- C. PLAYER STATS ----------
stats_rows = []
stat_mode = "date" if current_date else ("week" if current_week else "season")
print("Using stat mode:", stat_mode)

for idx, p in enumerate(players, start=1):
    pk = p["player_key"]
    if not pk:
        continue

    if stat_mode == "date":
        url = f"{ROOT}/player/{pk}/stats?date={current_date}&format=json"
    elif stat_mode == "week":
        url = f"{ROOT}/player/{pk}/stats;type=week;week={current_week}?format=json"
    else:
        url = f"{ROOT}/player/{pk}/stats?format=json"

    j = safe_get(url)
    if not j:
        stats_rows.append({
            "player_key": pk,
            "player_name": p["player_name"],
            "stat_id": None,
            "stat_value": None,
        })
        continue

    statlist = find_key(j, "stat")
    if statlist is None:
        stats_rows.append({
            "player_key": pk,
            "player_name": p["player_name"],
            "stat_id": None,
            "stat_value": None,
        })
    else:
        if isinstance(statlist, dict):
            statlist = [statlist]
        for s in statlist:
            sid = find_key(s, "stat_id") or s.get("stat_id")
            sval = find_key(s, "value") or s.get("value")
            if isinstance(sval, dict):
                sval = sval.get("value") or sval.get("display_value")
            stats_rows.append({
                "player_key": pk,
                "player_name": p["player_name"],
                "stat_id": sid,
                "stat_value": sval,
            })

    if idx % 10 == 0:
        time.sleep(0.6)
    else:
        time.sleep(0.15)

with open("player_stats.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["player_key","player_name","stat_id","stat_value"])
    w.writeheader()
    for r in stats_rows:
        w.writerow(r)
print("Wrote player_stats.csv with", len(stats_rows), "rows")

# ---------- D. TEAM ROSTERS ----------
league_teams_url = f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json"
teams_json = safe_get(league_teams_url)
team_roster_rows = []

if teams_json:
    try:
        teams_node = teams_json["fantasy_content"]["league"][1]["teams"]
    except Exception:
        teams_node = find_key(teams_json, "teams")

    if teams_node:
        team_count = int(teams_node.get("count", 0))
        team_keys = []
        for i in range(team_count):
            entry = teams_node.get(str(i))
            if not entry:
                continue
            try:
                t = entry["team"][0]
                team_key = t[0]["team_key"]
                team_name = t[2]["name"]
                team_keys.append((team_key, team_name))
            except Exception as e:
                print("Team parse error:", e)
        print("Found team keys:", team_keys)

        for tkey, tname in team_keys:
            roster_url = f"{ROOT}/team/{tkey}/roster?format=json"
            rjson = safe_get(roster_url)
            if not rjson:
                continue
            try:
                team_block = rjson["fantasy_content"]["team"][0]
                players_block = rjson["fantasy_content"]["team"][1]["roster"]["0"]["players"]
            except Exception:
                players_block = find_key(rjson, "players")

            if not players_block:
                print("No players in roster for", tkey)
                continue

            if isinstance(players_block, dict):
                for k, v in players_block.items():
                    if k == "count":
                        continue
                    try:
                        pw = v["player"][0]  # list of small dicts
                    except Exception:
                        continue
                    pkey = None
                    pname = None
                    pos = None
                    for item in pw:
                        if not isinstance(item, dict):
                            continue
                        if "player_key" in item and not pkey:
                            pkey = item["player_key"]
                        if "name" in item and not pname:
                            nm = item["name"]
                            if isinstance(nm, dict):
                                pname = nm.get("full")
                            else:
                                pname = nm
                        if "display_position" in item and not pos:
                            pos = item["display_position"]
                    team_roster_rows.append({
                        "team_key": tkey,
                        "team_name": tname,
                        "player_key": pkey,
                        "player_name": pname,
                        "position": pos,
                    })
            time.sleep(0.15)

with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["team_key","team_name","player_key","player_name","position"])
    w.writeheader()
    for r in team_roster_rows:
        w.writerow(r)
print("Wrote team_rosters.csv with", len(team_roster_rows), "rows")

# ---------- E. STANDINGS ----------
stand_url = f"{ROOT}/league/{LEAGUE_KEY}/standings?format=json"
stand_json = safe_get(stand_url)
stand_rows = []

if stand_json:
    try:
        st_teams = stand_json["fantasy_content"]["league"][1]["standings"]["0"]["teams"]
        st_count = int(st_teams.get("count", 0))
        for i in range(st_count):
            t = st_teams[str(i)]["team"]
            tk = t[0]["team_key"]
            tname = t[2]["name"]
            ts = t[3]["team_standings"]
            outcome = ts["outcome_totals"]
            stand_rows.append({
                "team_key": tk,
                "team_name": tname,
                "wins": outcome.get("wins"),
                "losses": outcome.get("losses"),
                "ties": outcome.get("ties"),
                "win_pct": outcome.get("percentage"),
                "rank": ts.get("rank"),
            })
    except Exception as e:
        print("Standings parse error:", e)

with open("standings.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["team_key","team_name","wins","losses","ties","win_pct","rank"])
    w.writeheader()
    for r in stand_rows:
        w.writerow(r)
print("Wrote standings.csv with", len(stand_rows), "rows")

# ---------- F. COMBINED PLAYER VIEW ----------
df_stats = pd.read_csv("player_stats.csv", dtype=str)
df_rosters = pd.read_csv("team_rosters.csv", dtype=str) if os.path.exists("team_rosters.csv") else pd.DataFrame(columns=["player_key","team_key","team_name","position"])

df_stats = df_stats.rename(columns=str.strip)
df_rosters = df_rosters.rename(columns=str.strip)

merged = pd.merge(
    df_stats,
    df_rosters[["player_key","team_key","team_name","position"]],
    how="left",
    on="player_key"
)

cols = ["player_key","player_name","team_key","team_name","position","stat_id","stat_value"]
merged = merged[[c for c in cols if c in merged.columns]]

merged.to_csv("combined_player_view.csv", index=False)
print("Wrote combined_player_view.csv with", len(merged), "rows")
