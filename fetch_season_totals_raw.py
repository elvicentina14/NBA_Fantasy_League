from yahoo_oauth import OAuth2
import pandas as pd
import os
from datetime import date
from yahoo_api import yahoo_get

SNAPSHOT_DATE = date.today().isoformat()

def ensure_list(x):
    if isinstance(x, list):
        return x
    return [x]

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")

    rosters = pd.read_csv("team_rosters.csv", dtype=str)
    player_keys = sorted(rosters["player_key"].unique())

    print(f"Fetching season totals for {len(player_keys)} players")

    rows = []

    for pk in player_keys:
        data = yahoo_get(oauth, f"player/{pk}/stats;type=season")
        player = data["player"]

        stats = ensure_list(player[1]["player_stats"]["stats"]["stat"])

        for s in stats:
            rows.append({
                "player_key": pk,
                "stat_id": s["stat_id"],
                "stat_value": s.get("value"),
                "snapshot_date": SNAPSHOT_DATE
            })

    df = pd.DataFrame(rows)

    if os.path.exists("player_season_totals.csv"):
        old = pd.read_csv("player_season_totals.csv", dtype=str)
        df = pd.concat([old, df], ignore_index=True)

    df.to_csv("player_season_totals.csv", index=False)
    print(f"✅ Wrote {len(df)} rows → player_season_totals.csv")

if __name__ == "__main__":
    main()
