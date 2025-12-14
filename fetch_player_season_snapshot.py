from yahoo_oauth import OAuth2
import os, csv
from datetime import datetime, timezone

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

timestamp = datetime.now(timezone.utc).isoformat()

# Load players
players = []
with open("league_players.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        players.append(r["player_key"])

rows = []

for idx, player_key in enumerate(players, start=1):
    print(f"[{idx}/{len(players)}] Fetching season stats for {player_key}")

    url = f"{ROOT}/player/{player_key}/stats;type=season?format=json"
    data = s.get(url).json()

    player_node = data["fantasy_content"]["player"][0]

    player_name = None
    stats_frag = None

    for frag in player_node:
        if not isinstance(frag, dict):
            continue

        if "name" in frag:
            player_name = frag["name"]["full"]

        if "player_stats" in frag:
            for ps_frag in frag["player_stats"]:
                if isinstance(ps_frag, dict) and "stats" in ps_frag:
                    stats_frag = ps_frag["stats"]

    if not stats_frag:
        continue

    # stats_frag is a LIST
    for stat_entry in stats_frag:
        if not isinstance(stat_entry, dict):
            continue

        stat = stat_entry.get("stat")
        if not stat:
            continue

        rows.append({
            "timestamp": timestamp,
            "player_key": player_key,
            "player_name": player_name,
            "stat_id": stat.get("stat_id"),
            "stat_value": stat.get("value"),
        })

# Write snapshot
file_exists = os.path.exists("fact_player_season_snapshot.csv")

with open("fact_player_season_snapshot.csv", "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["timestamp", "player_key", "player_name", "stat_id", "stat_value"]
    )
    if not file_exists:
        writer.writeheader()
    writer.writerows(rows)

print(f"Appended {len(rows)} rows to fact_player_season_snapshot.csv")
