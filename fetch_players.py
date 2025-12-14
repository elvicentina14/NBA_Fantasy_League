import os, csv, sys
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    sys.exit("LEAGUE_KEY not set")

oauth = OAuth2(None, None, from_file="oauth2.json")
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

def get(url):
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

players = []
start = 0
count = 25

while True:
    url = f"{ROOT}/league/{LEAGUE_KEY}/players;start={start};count={count}?format=json"
    data = get(url)
    container = data["fantasy_content"]["league"][1]["players"]

    for k, v in container.items():
        if not k.isdigit():
            continue
        plist = v["player"][0]
        row = {"player_key": None, "player_id": None, "player_name": None}

        for item in plist:
            if not isinstance(item, dict):
                continue
            if "player_key" in item:
                row["player_key"] = item["player_key"]
            if "player_id" in item:
                row["player_id"] = item["player_id"]
            if "name" in item:
                row["player_name"] = item["name"]["full"]

        if row["player_key"]:
            players.append(row)

    if len(players) < start + count:
        break
    start += count

with open("league_players.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f, fieldnames=["player_key", "player_id", "player_name"]
    )
    writer.writeheader()
    writer.writerows(players)

print(f"Wrote league_players.csv rows: {len(players)}")
