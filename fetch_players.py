import csv
import os
import requests
from yahoo_oauth import OAuth2
from datetime import datetime

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"
OUTFILE = "league_players.csv"

oauth = OAuth2(None, None, from_file="oauth2.json")

def first_dict(x):
    """Yahoo often wraps dicts inside single-item lists"""
    if isinstance(x, list):
        return x[0] if x else {}
    return x or {}

def extract_players(players_block):
    """
    players_block example:
    {
      "count": 25,
      "0": {"player": [...]},
      "1": {"player": [...]}
    }
    """
    rows = []

    for k, v in players_block.items():
        if not k.isdigit():
            continue

        wrapper = first_dict(v)
        player = first_dict(wrapper.get("player", []))

        name = first_dict(player.get("name", []))

        rows.append({
            "player_key": player.get("player_key"),
            "player_id": player.get("player_id"),
            "editorial_player_key": player.get("editorial_player_key"),
            "player_name": name.get("full"),
            "position_type": player.get("position_type"),
            "eligible_positions": ",".join(
                p["position"]
                for p in player.get("eligible_positions", [])
                if "position" in p
            ),
            "timestamp": datetime.utcnow().isoformat()
        })

    return rows

all_rows = []
start = 0
count = 25

while True:
    url = f"{BASE_URL}/league/{LEAGUE_KEY}/players;start={start};count={count}?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    data = r.json()

    league = first_dict(
        first_dict(data["fantasy_content"]["league"])
    )

    players_block = first_dict(league.get("players", {}))
    page_count = int(players_block.get("count", 0))

    # ✅ IMPORTANT FIX: do NOT stop on empty pages after page 0
    if page_count == 0:
        if start == 0:
            print("No players returned on first page; aborting")
            break
        else:
            print(f"No players on page start={start}; skipping")
            start += count
            continue

    rows = extract_players(players_block)
    all_rows.extend(rows)

    start += count

# --- WRITE OUTPUT ---
if all_rows:
    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)

print(f"Wrote {len(all_rows)} rows to {OUTFILE}")
