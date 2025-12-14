# fetch_player_season_snapshot.py

import csv
import os
from datetime import datetime
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_FILE = "fact_player_season_snapshot.csv"


def get_oauth():
    return OAuth2(None, None, from_file="oauth2.json")


def main():
    oauth = get_oauth()
    ts = datetime.utcnow().isoformat()

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players/stats"
    r = oauth.session.get(url, params={"format": "json"})
    r.raise_for_status()
    data = r.json()

    league = first_dict(data["fantasy_content"]["league"])
    players = as_list(league.get("players", {}).get("player"))

    rows = []

    for p in players:
        pdata = first_dict(p)
        stats = as_list(pdata.get("player_stats", {}).get("stats", {}).get("stat"))

        for s in stats:
            stat = first_dict(s)
            rows.append({
                "snapshot_ts": ts,
                "player_key": pdata.get("player_key"),
                "stat_id": stat.get("stat_id"),
                "value": stat.get("value"),
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
