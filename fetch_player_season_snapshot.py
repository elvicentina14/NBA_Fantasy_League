# fetch_player_season_snapshot.py

import csv
import os
from datetime import datetime, timezone
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all, extract_fragment

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TS = datetime.now(timezone.utc).isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")

players = []
with open("league_players.csv", encoding="utf-8") as f:
    players = list(csv.DictReader(f))

rows = []

for p in players:
    pk = p["player_key"]
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    data = r.json()

    stats_blocks = find_all(data, "stats")
    for sb in stats_blocks:
        stats = find_all(sb, "stat")
        for s in stats:
            rows.append({
                "timestamp": TS,
                "player_key": pk,
                "stat_id": extract_fragment(s, "stat_id"),
                "value": extract_fragment(s, "value"),
            })

if not rows:
    print("WARNING: No stats rows extracted")

with open("fact_player_season_snapshot.csv", "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["timestamp", "player_key", "stat_id", "value"]
    )
    if f.tell() == 0:
        writer.writeheader()
    writer.writerows(rows)

print(f"Appended {len(rows)} rows to fact_player_season_snapshot.csv")
