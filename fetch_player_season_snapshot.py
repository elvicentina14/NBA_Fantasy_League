# fetch_player_season_snapshot.py
from yahoo_oauth import OAuth2
import csv, os
from datetime import datetime, timezone
from yahoo_normalize import first

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
PLAYERS_CSV = "league_players.csv"
OUT = "fact_player_season_snapshot.csv"
SNAPSHOT_TS = datetime.now(timezone.utc).isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")

rows = []

with open(PLAYERS_CSV, newline="", encoding="utf-8") as f:
    players = list(csv.DictReader(f))

for idx, p in enumerate(players, 1):
    print(f"[{idx}/{len(players)}] {p['player_key']}")

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{p['player_key']}/stats?format=json"
    data = oauth.session.get(url).json()

    player = first(first(data["fantasy_content"]["player"]))
    stats_block = first(player.get("player_stats"))
    stats = first(stats_block.get("stats"))
    stat_list = stats.get("stat", [])

    for s in stat_list:
        stat = first(s)
        rows.append({
            "snapshot_ts": SNAPSHOT_TS,
            "player_key": p["player_key"],
            "stat_id": stat.get("stat_id"),
            "stat_value": stat.get("value"),
        })

file_exists = os.path.exists(OUT)

with open(OUT, "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    if not file_exists:
        writer.writeheader()
    writer.writerows(rows)

print(f"Appended {len(rows)} rows to {OUT}")
