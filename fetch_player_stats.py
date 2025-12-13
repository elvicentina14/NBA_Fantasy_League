from yahoo_oauth import OAuth2
import pandas as pd
import os
from datetime import datetime

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
DATE = datetime.utcnow().strftime("%Y-%m-%d")

oauth = OAuth2(None, None, from_file="oauth2.json")

print("Loading players from team_rosters.csv")
players = pd.read_csv("team_rosters.csv")["player_key"].unique().tolist()

rows = []

for i, pk in enumerate(players, 1):
    print(f"[{i}/{len(players)}] {pk}")

    resp = oauth.session.get(
        f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats?format=json"
    ).json()

    try:
        player = resp["fantasy_content"]["player"]
        name = player[0]["name"]["full"]
        stats = player[1]["player_stats"]["stats"]["stat"]

        for s in stats:
            rows.append({
                "player_key": pk,
                "player_name": name,
                "date": DATE,
                "stat_id": s["stat_id"],
                "stat_value": s["value"],
            })
    except Exception:
        continue

df = pd.DataFrame(rows)
os.makedirs("player_stats_daily", exist_ok=True)
df.to_csv(f"player_stats_daily/{DATE}.csv", index=False)

print(f"✅ Wrote {len(df)} stat rows")
