# fetch_players.py
import os, sys, logging, time
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all
from safe_io import safe_write_csv, debug_dump
from http_helpers import safe_get

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    logging.error("LEAGUE_KEY env var not set")
    sys.exit(2)

OUT = "league_players.csv"
oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

rows = []
start = 0
count = 25

DEBUG_DUMP = os.environ.get("DEBUG_DUMP") == "1"

while True:
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/players;start={start};count={count}?format=json"
    logging.info("GET %s", url)
    status, data = safe_get(session, url)
    if DEBUG_DUMP and start == 0:
        debug_dump(data, "debug_players_page0.json")

    # league is list: index 1 typically contains containers
    league_list = as_list(data.get("fantasy_content", {}).get("league"))
    if len(league_list) < 2:
        logging.warning("Unexpected league structure, stopping pagination")
        break

    # Collect any 'player' entries found anywhere under league_list[1]
    players_found = []
    players_candidates = find_all(league_list[1], "player")
    for cand in players_candidates:
        # cand might be a list of player wrappers or a single wrapper
        if isinstance(cand, list):
            for wrapper in cand:
                players_found.append(wrapper)
        else:
            players_found.append(cand)

    if not players_found:
        # If no players found for this page, stop
        logging.info("No players found on page start=%d; stopping", start)
        break

    # Each wrapper may be a list of fragments OR a dict (safe)
    for wrapper in players_found:
        frag_list = wrapper if isinstance(wrapper, list) else [wrapper]
        # Search fragments to gather canonical fields
        player_key = None
        editorial_player_key = None
        player_id = None
        player_name = None

        for frag in frag_list:
            if not isinstance(frag, dict):
                continue
            if "player_key" in frag:
                player_key = frag.get("player_key")
                editorial_player_key = frag.get("editorial_player_key")
                player_id = frag.get("player_id")
            if "name" in frag:
                name_frag = first_dict(frag.get("name"))
                player_name = name_frag.get("full") or player_name

        if player_key:
            rows.append({
                "player_key": player_key,
                "player_id": player_id,
                "editorial_player_key": editorial_player_key,
                "player_name": player_name,
            })

    # Pagination advance
    start += count
    # small delay to be friendly to API
    time.sleep(0.12)

# Safety guard: write only if rows exist
if not rows:
    logging.warning("No players parsed — skipping write")
    sys.exit(0)

fieldnames = ["player_key", "player_id", "editorial_player_key", "player_name"]
n = safe_write_csv(OUT, rows, fieldnames, mode="w")
logging.info("Wrote %d rows to %s", n, OUT)
