import csv
import datetime
import os
from yahoo_oauth import OAuth2
from yahoo_utils import list_to_dict, safe_get

LEAGUE_KEY = os.environ["LEAGUE_KEY"]
RUN_TS = datetime.datetime.utcnow().isoformat()

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session


def extract_roster_players(roster_block):
    roster_dict = list_to_dict(roster_block)

    if "players" in roster_dict:
        return list_to_dict(roster_dict["players"]).get("player", [])

    for v in roster_dict.values():
        if isinstance(v, dict) and "players" in v:
            return list_to_dict(v["players"]).get("player", [])

    return []


rows = []

teams_url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
teams_data = session.get(teams_url).json()

league = list_to_dict(teams_data["fantasy_content"]["league"])
teams = list_to_dict(league["teams"]).get("team", [])

for t in teams:
    tea
