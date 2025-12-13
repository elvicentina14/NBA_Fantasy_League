from yahoo_oauth import OAuth2
import pandas as pd
import os

LEAGUE_KEY = os.environ["LEAGUE_KEY"]

def as_list(x):
    if isinstance(x, list):
        return x
    if x is None:
        return []
    return [x]

def unwrap(x):
    if isinstance(x, list):
        return x[0]
    return x

oauth = OAuth2(None, None, from_file="oauth2.json")

print("Fetching league players...")
resp = oauth.session.get(
    f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players?format=json"
).json()

league = unwrap(resp["fantasy_content"]["league"])
players = as_list(league["players"]["player"])

rows = []

for i, p in enumerate(players, 1):
    player = unwrap(p)
    print(f"[{i}/{len(players)}] {player['name']['full']}")

    stats_resp = oauth.session.get(
        f"https://fantasysports.yahooapis.com/fantasy/v2/player/{player['player_key']}/stats?format=json"
    ).json()

    stats = unwrap(
        unwrap(stats_resp["fantasy_content"]["player"])["player_stats"]
    )["stats"]["stat"]

    for s in as_list(stats):
        rows.append({
            "player_key": player["player_key"],
            "player_name": player["name"]["full"],
            "stat_id": s["stat_id"],
            "stat_value": s.get("value"),
        })

pd.DataFrame(rows).to_csv("player_stats_season.csv", index=False)
print(f"✅ Wrote {len(rows)} season stat rows")
