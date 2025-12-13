import os
import csv
from datetime import date
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import League

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TODAY = date.today().isoformat()
OUTDIR = "player_stats_daily"

os.makedirs(OUTDIR, exist_ok=True)

def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def list_to_dict(node):
    out = {}
    for item in node:
        if isinstance(item, dict):
            out.update(item)
    return out

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    league = League(oauth, LEAGUE_KEY)

    players = league.players()

    rows = []

    for i, p in enumerate(players, 1):
        print(f"[{i}/{len(players)}] Fetching stats for {p['player_key']}")

        stats = league.player_stats(p["player_key"], "date", TODAY)

        stat_block = stats.get("player_stats", {}).get("stats", {}).get("stat")
        stat_block = ensure_list(stat_block)

        for s in stat_block:
            sdict = list_to_dict(s)
            rows.append({
                "player_key": p["player_key"],
                "player_name": p.get("name"),
                "timestamp": TODAY,
                "stat_id": sdict.get("stat_id"),
                "stat_value": sdict.get("value")
            })

    outfile = f"{OUTDIR}/{TODAY}.csv"

    if rows:
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["player_key", "player_name", "timestamp", "stat_id", "stat_value"]
            )
            writer.writeheader()
            writer.writerows(rows)

        print(f"✅ Wrote {len(rows)} rows → {outfile}")
    else:
        print("⚠️ No stats returned by Yahoo today")

if __name__ == "__main__":
    main()
