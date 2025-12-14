import os, csv
from datetime import datetime, timezone
from yahoo_oauth import OAuth2
from yahoo_utils import iter_indexed_dict, merge_fragments

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
SNAPSHOT_DATE = datetime.now(timezone.utc).date().isoformat()
ROOT = "https://fantasysports.yahooapis.com/fantasy/v2"

oauth = OAuth2(None, None, from_file="oauth2.json")
s = oauth.session

rows = []

# --- fetch teams ---
teams_json = s.get(f"{ROOT}/league/{LEAGUE_KEY}/teams?format=json").json()
teams_node = teams_json["fantasy_content"]["league"][1]["teams"]

for t in iter_indexed_dict(teams_node):
    team_block = t["team"][0]
    team = merge_fragments(team_block)

    team_key = team.get("team_key")
    team_name = team.get("name")

    # --- fetch roster ---
    roster_json = s.get(f"{ROOT}/team/{team_key}/roster?format=json").json()
    players_node = roster_json["fantasy_content"]["team"][1]["roster"]["0"]["players"]

    for p in iter_indexed_dict(players_node):
        player_block = p["player"][0]
        player = merge_fragments(player_block)

        rows.append({
            "snapshot_date": SNAPSHOT_DATE,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": player.get("player_key"),
            "player_name": player.get("name", {}).get("full"),
            "position": player.get("display_position"),
        })

out_file = "fact_team_roster_snapshot.csv"
write_header = not os.path.exists(out_file)

with open(out_file, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(
        f,
        fieldnames=[
            "snapshot_date","team_key","team_name",
            "player_key","player_name","position"
        ]
    )
    if write_header:
        w.writeheader()
    w.writerows(rows)

print(f"Appended {len(rows)} rows to {out_file}")
