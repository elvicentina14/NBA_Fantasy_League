# fetch_team_roster_snapshot.py
import os, sys, logging, time
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all
from safe_io import safe_write_csv, debug_dump
from http_helpers import safe_get
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    logging.error("LEAGUE_KEY env var not set")
    sys.exit(2)

OUT = "fact_team_roster_snapshot.csv"
TS = datetime.now(timezone.utc).isoformat()
oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

rows = []
DEBUG_DUMP = os.environ.get("DEBUG_DUMP") == "1"

# Fetch teams list
url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams?format=json"
logging.info("GET teams %s", url)
status, data = safe_get(session, url)
if DEBUG_DUMP:
    debug_dump(data, "debug_teams.json")

league_list = as_list(data.get("fantasy_content", {}).get("league"))
if len(league_list) < 2:
    logging.error("Unexpected teams league structure")
    sys.exit(1)

teams_raw = find_all(league_list[1], "team")
# teams_raw items may be dicts or lists
team_wrappers = []
for t in teams_raw:
    if isinstance(t, list):
        # flatten: each element might be wrapper dicts
        for elt in t:
            team_wrappers.append(elt)
    else:
        team_wrappers.append(t)

# For each team wrapper, extract team_key and then fetch roster endpoint for canonical roster
for tw in team_wrappers:
    team_meta = first_dict(tw)
    team_key = team_meta.get("team_key")
    team_name = team_meta.get("name") or first_dict(team_meta.get("name", {})) if team_meta.get("name") else None

    if not team_key:
        continue

    # call roster endpoint to get canonical roster (less fragile)
    roster_url = f"https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster?format=json"
    logging.info("GET roster %s", roster_url)
    try:
        status, rdata = safe_get(session, roster_url)
    except Exception as e:
        logging.exception("Failed to fetch roster for %s", team_key)
        continue

    if DEBUG_DUMP:
        debug_dump(rdata, f"debug_roster_{team_key.replace('/', '_')}.json")

    team_block = as_list(rdata.get("fantasy_content", {}).get("team"))
    if len(team_block) < 2:
        logging.warning("Unexpected team block for %s", team_key)
        continue

    roster_block = first_dict(team_block[1]).get("roster")
    players_found = find_all(roster_block, "player")

    for p in players_found:
        # p may be wrapper list or dict
        frag_list = p if isinstance(p, list) else [p]
        player_key = None
        player_name = None
        position = None
        for frag in frag_list:
            if not isinstance(frag, dict):
                continue
            if "player_key" in frag:
                player_key = frag.get("player_key")
            if "name" in frag:
                player_name = first_dict(frag.get("name")).get("full")
            if "display_position" in frag:
                position = frag.get("display_position")

        if player_key:
            rows.append({
                "snapshot_ts": TS,
                "team_key": team_key,
                "team_name": team_name,
                "player_key": player_key,
                "player_name": player_name,
                "position": position
            })

    time.sleep(0.12)

if not rows:
    logging.info("No roster rows parsed — skipping write")
    sys.exit(0)

fieldnames = ["snapshot_ts", "team_key", "team_name", "player_key", "player_name", "position"]
n = safe_write_csv(OUT, rows, fieldnames, mode="a")
logging.info("Appended %d rows to %s", n, OUT)
