from yahoo_oauth import OAuth2
import pandas as pd
import os
from datetime import date

CONFIG = "oauth2.json"
LEAGUE_KEY = os.environ["LEAGUE_KEY"]
TODAY = date.today().isoformat()

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def find_all(obj, key, out):
    if isinstance(obj, dict):
        if key in obj:
            out.append(obj[key])
        for v in obj.values():
            find_all(v, key, out)
    elif isinstance(obj, list):
        for i in obj:
            find_all(i, key, out)

def yahoo(oauth, path):
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/{path}?format=json"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()

oauth = OAuth2(None, None, from_file=CONFIG)

# ---------- LEAGUE PLAYERS ----------
players_raw = yahoo(oauth, f"league/{LEAGUE_KEY}/players;count=2000")
players = []
find_all(players_raw, "player", players)

league_players = [{
    "player_key": p.get("player_key"),
    "player_name": p.get("name", {}).get("full")
} for p in players]

pd.DataFrame(league_players).drop_duplicates().to_csv(
    "league_players.csv", index=False
)

# ---------- STATS ----------
rows = []
for p in league_players:
    pk = p["player_key"]
    d = yahoo(oauth, f"player/{pk}/stats;type=date;date={TODAY}")

    stats = []
    find_all(d, "stat", stats)

    for s in stats:
        rows.append({
            "player_key": pk,
            "player_name": p["player_name"],
            "timestamp": TODAY,
            "stat_id": s.get("stat_id"),
            "stat_value": s.get("value"),
        })

os.makedirs("player_stats_daily", exist_ok=True)
df = pd.DataFrame(rows)
df.to_csv(f"player_stats_daily/{TODAY}.csv", index=False)

# ---------- PARQUET ----------
df["stat_value_num"] = pd.to_numeric(df["stat_value"], errors="coerce")
df.to_parquet("player_stats_full.parquet", index=False)

combined = (
    df.merge(
        pd.read_csv("team_rosters.csv"),
        on="player_key",
        how="left"
    )
)

combined.to_parquet("combined_player_view_full.parquet", index=False)
print("✔ stats + parquet written")
