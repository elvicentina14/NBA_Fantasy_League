# fetch_player_season_snapshot.py

import csv
import os
from datetime import datetime
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, merge_kv_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "fact_player_season_snapshot.csv"


def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    snapshot_ts = datetime.utcnow().isoformat()
    rows = []

    with open("league_players.csv", newline="", encoding="utf-8") as f:
        players = list(csv.DictReader(f))

    for p in players:
        player_key = p["player_key"]

        url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{player_key}/stats"
        r = oauth.session.get(url, params={"format": "json"})
        r.raise_for_status()
        data = r.json()

        player = merge_kv_list(data["fantasy_content"]["player"])
        stats_blocks = as_list(player.get("stats"))

        for sb in stats_blocks:
            stats = as_list(merge_kv_list(sb).get("stat"))
            for s in stats:
                rows.append({
                    "snapshot_ts": snapshot_ts,
                    "player_key": player_key,
                    "stat_id": s.get("stat_id"),
                    "value": s.get("value"),
                })

    if not rows:
        print("No stats found")
        return

    file_exists = os.path.exists(OUT_FILE)
    with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"Appended {len(rows)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
