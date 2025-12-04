from yahoo_oauth import OAuth2
import requests, pandas as pd, os, json

LEAGUE_KEY = os.getenv("LEAGUE_KEY")  # from GitHub secret

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

# ---- GET LEAGUE DETAILS TO EXTRACT TEAM KEYS ----
league_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
league_data = session.get(league_url).json()

teams_node = league_data["fantasy_content"]["league"][1]["teams"]
team_keys = [teams_node[str(i)]["team"][0][0]["team_key"] 
             for i in range(teams_node["count"])]

# ----------------- FETCH ROSTERS --------------------
all_rows = []

for tkey in team_keys:
    roster_url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{tkey}/roster?format=json"
    r = session.get(roster_url).json()

    team_data = r["fantasy_content"]["team"][0]
    team_key = team_data[0]["team_key"]
    team_name = team_data[2]["name"]

    players = r["fantasy_content"]["team"][1]["roster"]["0"]["players"]
    
    for p_index, p in players.items():
        player = p["player"][0]
        player_key = player[0]["player_key"]
        name = player[2]["name"]["full"]
        pos = player[9]["display_position"]

        all_rows.append({
            "team_key": team_key,
            "team_name": team_name,
            "player_key": player_key,
            "player_name": name,
            "position": pos
        })

pd.DataFrame(all_rows).to_csv("team_rosters.csv", index=False)

# ----------------- FETCH STANDINGS --------------------
stand_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/standings?format=json"
stand_data = session.get(stand_url).json()

stand_teams = stand_data["fantasy_content"]["league"][1]["standings"]["0"]["teams"]
rows = []

for i in range(stand_teams["count"]):
    t = stand_teams[str(i)]["team"]
    tk = t[0]["team_key"]
    name = t[2]["name"]
    wlt = t[3]["team_standings"]["outcome_totals"]
    rank = t[3]["team_standings"]["rank"]

    rows.append({
        "team_key": tk,
        "team_name": name,
        "wins": wlt["wins"],
        "losses": wlt["losses"],
        "ties": wlt["ties"],
        "win_pct": wlt["percentage"],
        "rank": rank
    })

pd.DataFrame(rows).to_csv("standings.csv", index=False)
print("Done — team_rosters.csv & standings.csv created.")
