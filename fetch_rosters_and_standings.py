import os, sys, csv, time
from yahoo_oauth import OAuth2
from yahoo_helpers import flatten_list, extract_name, canonical_player_key

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


league = get(f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json")
teams = league["fantasy_content"]["league"][1]["teams"]

team_keys = [
    teams[str(i)]["team"][0][0]["team_key"]
    for i in range(int(teams["count"]))
]

rows = []

for tkey in team_keys:
    j = get(f"{ROOT}/team/{tkey}/roster?format=json")
    if not j:
        continue

    team = j["fantasy_content"]["team"]
    team_key = team[0][0]["team_key"]
    team_name = team[0][2]["name"]

    players = team[1]["roster"]["0"]["players"]

    for k, entry in players.items():
        if k == "count":
            continue
        wrapper = entry.get("player") if isinstance(entry, dict) else None
        if not wrapper:
            continue

        wrapper = flatten_list(wrapper)

        pk = pid = epk = name = pos = None
        for item in wrapper:
            if not isinstance(item, dict):
                continue
            pk = pk or item.get("player_key")
            pid = pid or item.get("player_id")
            epk = epk or item.get("editorial_player_key")
            pos = pos or item.get("display_position")
            name = name or extract_name(item)

        final_key = canonical_player_key(pk, pid) or epk

        rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "player_key": final_key,
            "player_name": name,
            "position": pos
        })

    time.sleep(0.2)

with open("team_rosters.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=["team_key", "team_name", "player_key", "player_name", "position"]
    )
    w.writeheader()
    w.writerows(rows)

print("Wrote team_rosters.csv rows:", len(rows))
