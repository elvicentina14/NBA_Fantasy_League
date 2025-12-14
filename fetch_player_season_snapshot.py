from yahoo_oauth import OAuth2
import os, csv
from datetime import datetime, timezone
from yahoo_utils import as_list, first_dict

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TS = datetime.now(timezone.utc).isoformat()
OUT = "fact_player_season_snapshot.csv"

oauth = OAuth2(None, None, from_file="oauth2.json")

players = []
with open("league_players.csv", newline="", encoding="utf-8") as f:
    players = list(csv.DictReader(f))

rows = []

for p in players:
    pk = p["player_key"]
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats?format=json"
    data = oauth.session.get(url).json()

    player = as_list(data["fantasy_content"]["player"])
    stats_block = first_dict(player[1]).get("player_stats")
    stats = as_list(first_dict(stats_block).get("stats", {}).get("stat"))

    for s_raw in stats:
        s = first_dict(s_raw)
        rows.append({
            "snapshot_ts": TS,
            "player_key": pk,
            "stat_id": s.get("stat_id"),
            "stat_value": s.get("value"),
        })

if rows:
    write_header = not os.path.exists(OUT)
    with open(OUT, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        if write_header:
            w.writeheader()
        w.writerows(rows)

print(f"Appended {len(rows)} rows to {OUT}")
