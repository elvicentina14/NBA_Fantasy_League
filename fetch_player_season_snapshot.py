import os
import csv
from datetime import datetime, timezone
from yahoo_oauth import OAuth2
from yahoo_utils import first_dict, find_all

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TS = datetime.now(timezone.utc).isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")

def yahoo_get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

players = []

with open("league_players.csv", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        players.append(row)

rows = []

for i, p in enumerate(players, start=1):
    print(f"[{i}/{len(players)}] Getting stats for {p['player_key']}")

    url = (
        f"https://fantasysports.yahooapis.com/fantasy/v2/"
        f"player/{p['player_key']}/stats?format=json"
    )

    data = yahoo_get(url)
    stats = find_all(data, "stat")

    stat_map = {}
    for s in stats:
        stat_id = s.get("stat_id")
        value = s.get("value")
        stat_map[stat_id] = value

    row = {
        "timestamp": TS,
        "player_key": p["player_key"],
        "player_id": p["player_id"],
        "player_name": p["player_name"],
        **stat_map,
    }
    rows.append(row)

with open("fact_player_season_snapshot.csv", "a", newline="", encoding="utf-8") as f:
    if rows:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if f.tell() == 0:
            writer.writeheader()
        writer.writerows(rows)
