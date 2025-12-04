# fetch_league_players.py
import os, json, csv, sys, time
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    print("ERROR: LEAGUE_KEY not set"); sys.exit(2)

oauth = OAuth2(None, None, from_file="oauth2.json")
def safe_get(url, params=None):
    r = oauth.session.get(url, params=params)
    print("GET", r.status_code, url, "params=", params)
    if r.status_code != 200:
        print("HTTP", r.status_code, r.text[:500])
        return None
    return r.json()

# try league players endpoint (status=ALL includes rostered)
url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players"
params = {"format":"json", "status":"ALL"}
data = safe_get(url, params=params)
if not data:
    print("No data from league players endpoint"); sys.exit(3)

with open("league_players.json","w",encoding="utf-8") as f:
    json.dump(data,f,indent=2,ensure_ascii=False)
print("Saved league_players.json")

# attempt to find player entries
def find_players(obj, out=None):
    if out is None: out=[]
    if isinstance(obj, dict):
        if "player" in obj and isinstance(obj["player"], list):
            out.extend(obj["player"])
        for v in obj.values():
            find_players(v,out)
    elif isinstance(obj, list):
        for e in obj:
            find_players(e,out)
    return out

players = find_players(data)
print("Players found count:", len(players))

# flatten some fields
rows=[]
for p in players:
    pid = None; pname=None; team=None
    if isinstance(p, dict):
        pid = p.get("player_id") or p.get("player",{}).get("player_id") if isinstance(p.get("player"),dict) else None
        name = p.get("name") or (p.get("player",{}).get("name") if isinstance(p.get("player"),dict) else None)
        if isinstance(name, dict):
            pname = name.get("full") or str(name)
        else:
            pname = name
        # some players include editorial team or ownership info
    rows.append({"player_id":pid,"player_name":pname})
with open("league_players.csv","w",newline="",encoding="utf-8") as f:
    import csv
    w=csv.DictWriter(f,fieldnames=["player_id","player_name"])
    w.writeheader()
    for r in rows: w.writerow(r)
print("Wrote league_players.csv rows:", len(rows))
