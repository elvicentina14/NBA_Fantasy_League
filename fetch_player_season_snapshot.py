# fetch_player_season_snapshot.py
import csv
import os
from datetime import datetime
from yahoo_oauth import OAuth2
from yahoo_utils import merge_kv_list, as_list

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "fact_player_season_snapshot.csv"

def oauth():
    return OAuth2(None, None, from_file="oauth2.json")

def main():
    session = oauth().session
    ts = datetime.utcnow().isoformat()

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players/stats"
    r = session.get(url, params={"format": "json"})
    r.raise_for_status()
    data = r.json()

    league = merge_kv_list(data["fantasy_content"]["league"])
    players_block = merge_kv_list(league["players"])
    players = as_list(players_block["player"])

    rows = []

    for p in players:
        pdata = merge_kv_list(p)
        stats_block = merge_kv_list(pdata["player_stats"])
        stats_container = merge_kv_list(stats_block["stats"])
        stats = as_list(stats_container["stat"])

        for s in stats:
            stat = merge_kv_list(s)
            rows.append({
                "snapshot_ts": ts,
                "player_key": pdata["player_key"],
                "stat_id": stat["stat_id"],
                "value": stat["value"],
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

    print(f"Appended {len(rows)} rows")

if __name__ == "__main__":
    main()
