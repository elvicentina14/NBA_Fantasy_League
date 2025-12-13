import os
import csv
from datetime import date
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import League, Player

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT_DIR = "player_stats_daily"
os.makedirs(OUT_DIR, exist_ok=True)

def main():
    oauth = OAuth2(None, None, from_file="oauth2.json")
    league = League(oauth, LEAGUE_KEY)

    team_keys = league.teams()
    seen_players = set()
    rows = []

    for team_key in team_keys:
        team = league.to_team(team_key)
        roster = team.roster()

        for p in roster:
            pk = p["player_key"]
            if pk in seen_players:
                continue
            seen_players.add(pk)

            player = Player(oauth, pk)
            stats = player.stats()

            for stat_id, stat_val in stats.items():
                rows.append({
                    "player_key": pk,
                    "player_name": p["name"]["full"],
                    "stat_id": stat_id,
                    "stat_value": stat_val,
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
