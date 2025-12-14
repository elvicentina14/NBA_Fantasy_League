import os, csv
from datetime import datetime, timezone
from yahoo_oauth import OAuth2
from yahoo_utils import iter_indexed_dict, merge_fragments, ensure_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
SNAPSHOT_DATE = datetime.now(timezone.utc).date().isoformat()
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

rows = []

url = f"{ROOT}/league/{LEAGUE_KEY}/players/stats;type=season?format=json"
j = s.get(url).json()

players_node = j["fantasy_content"]["league"][1]["players"]

for p in iter_indexed_dict(players_node):
    player_sections = p["player"]

    # --- identity ---
    player_identity = merge_fragments(player_sections[0])
    player_key = player_identity.get("player_key")
    player_name = player_identity.get("name", {}).get("full")

    # --- stats section ---
    stats_container = player_sections[1].get("player_stats", [])
    stats_fragments = ensure_list(stats_container)

    merged_stats = merge_fragments(stats_fragments)
    stats_list = (
        merged_stats
        .get("stats", {})
        .get("stat", [])
    )

    for stat in ensure_list(stats_list):
        rows.append({
            "snapshot_date": SNAPSHOT_DATE,
            "player_key": player_key,
            "player_name": player_name,
            "stat_id": stat.get("stat_id"),
            "season_total": stat.get("value"),
        })

out_file = "fact_player_season_snapshot.csv"
write_header = not os.path.exists(out_file)

with open(out_file, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=[
            "snapshot_date",
            "player_key",
            "player_name",
            "stat_id",
            "season_total"
        ]
    )
    if write_header:
        w.writeheader()
    w.writerows(rows)

print(f"Appended {len(rows)} rows to {out_file}")
