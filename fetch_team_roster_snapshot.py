from yahoo_oauth import OAuth2
import os, csv
from datetime import datetime, timezone
from yahoo_utils import as_list, first_dict

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
OUT = "fact_team_roster_snapshot.csv"
TS = datetime.now(timezone.utc).isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")

rows = []

league_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
data = oauth.session.get(league_url).json()

league = as_list(data["fantasy_content"]["league"])
teams_block = first_dict(league[1]).get("teams")
teams = as_list(first_dict(teams_block).get("team"))

for t_raw in teams:
    team_meta = first_dict(t_raw)
    team_key = team_meta.get("team_key")
    team_name = team_meta.get("name")

    roster_url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    rdata = oauth.session.get(roster_url).json()

    team = as_list(rdata["fantasy_content"]["team"])
    roster_block = first_dict(team[1]).get("roster")
    players = as_list(first_dict(roster_block).get("players", {}).get("player"))

    for p_raw in players:
        p = first_dict(p_raw)
        name = first_dict(p.get("name"))

        rows.append({
            "snapshot_ts": TS,
            "team_key": team_key,
            "team_name": team_name,
            "player_key": p.get("player_key"),
            "player_name": name.get("full"),
            "position": p.get("display_position"),
        })

write_header = not os.path.exists(OUT)
with open(OUT, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    if write_header:
        w.writeheader()
    w.writerows(rows)

print(f"Appended {len(rows)} rows to {OUT}")
