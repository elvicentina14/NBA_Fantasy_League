import os, csv
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

data = get(f"{ROOT}/league/{LEAGUE_KEY}/players/stats;type=season?format=json")
players = data["fantasy_content"]["league"][1]["players"]

rows = []

for _, p in players.items():
    if not isinstance(p, dict):
        continue

    plist = p["player"][0]
    base = {"player_key": None, "player_name": None}

    for item in plist:
        if isinstance(item, dict):
            if "player_key" in item:
                base["player_key"] = item["player_key"]
            if "name" in item:
                base["player_name"] = item["name"]["full"]

    stats = p["player"][1]["player_stats"]["stats"]["stat"]
    for s in stats:
        rows.append({
            **base,
            "stat_id": s["stat_id"],
            "stat_value": s["value"]
        })

with open("player_stats_season.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f, fieldnames=["player_key", "player_name", "stat_id", "stat_value"]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote player_stats_season.csv rows: {len(rows)}")
