import os
import csv
from datetime import date
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import League

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_DIR = "player_stats_daily"
os.makedirs(OUT_DIR, exist_ok=True)

def find_players(node, found):
    if isinstance(node, dict):
        if "player_key" in node:
            found.append(node)
        for v in node.values():
            find_players(v, found)
    elif isinstance(node, list):
        for i in node:
            find_players(i, found)

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    league = League(oauth, LEAGUE_KEY)

    raw = league.league_raw
    players = []
    find_players(raw, players)

    rows = []

    for p in players:
        stats = p.get("player_stats", {}).get("stats", [])
        for s in stats:
            rows.append({
                "player_key": p.get("player_key"),
                "player_name": p.get("name", {}).get("full", ""),
                "stat_id": s.get("stat_id"),
                "stat_value": s.get("value"),
                "date": str(date.today())
            })

    out_file = os.path.join(OUT_DIR, f"{date.today()}.csv")

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["player_key", "player_name", "stat_id", "stat_value", "date"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows → {out_file}")

if __name__ == "__main__":
    main()
