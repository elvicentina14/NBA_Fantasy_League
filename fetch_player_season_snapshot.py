# fetch_player_season_snapshot.py
import os, sys, logging, time
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all
from safe_io import safe_write_csv, debug_dump
from http_helpers import safe_get
from datetime import datetime, timezone
import csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    logging.error("LEAGUE_KEY env var not set")
    sys.exit(2)

PLAYERS_CSV = "league_players.csv"
OUT = "fact_player_season_snapshot.csv"
TS = datetime.now(timezone.utc).isoformat()
oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

DEBUG_DUMP = os.environ.get("DEBUG_DUMP") == "1"

# Load players
if not os.path.exists(PLAYERS_CSV):
    logging.error("%s not found, run fetch_players.py first", PLAYERS_CSV)
    sys.exit(1)

players = []
with open(PLAYERS_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        if r.get("player_key"):
            players.append(r)

if not players:
    logging.info("No players in %s", PLAYERS_CSV)
    sys.exit(0)

rows = []
for idx, p in enumerate(players, start=1):
    pk = p["player_key"]
    logging.info("[%d/%d] Getting stats for %s", idx, len(players), pk)
    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats?format=json"
    try:
        status, data = safe_get(session, url)
    except Exception as e:
        logging.exception("Failed to fetch stats for %s", pk)
        continue

    if DEBUG_DUMP and idx <= 5:
        debug_dump(data, f"debug_stats_{pk}.json")

    # find player node and its player_stats fragment(s)
    player_nodes = as_list(data.get("fantasy_content", {}).get("player"))
    if len(player_nodes) < 2:
        logging.debug("Unexpected player node for %s", pk)
        continue

    stats_frag = first_dict(player_nodes[1]).get("player_stats")
    # stats_frag may be list/dict; find all "stat" occurrences under it
    stat_lists = find_all(stats_frag, "stat")
    # flatten: each entry may be list of stat dicts or dict
    for sl in stat_lists:
        if isinstance(sl, list):
            for stat_item in sl:
                s = first_dict(stat_item)
                if s.get("stat_id") is not None:
                    rows.append({
                        "snapshot_ts": TS,
                        "player_key": pk,
                        "stat_id": s.get("stat_id"),
                        "stat_value": s.get("value")
                    })
        elif isinstance(sl, dict):
            s = first_dict(sl)
            if s.get("stat_id") is not None:
                rows.append({
                    "snapshot_ts": TS,
                    "player_key": pk,
                    "stat_id": s.get("stat_id"),
                    "stat_value": s.get("value")
                })
    # polite pause
    time.sleep(0.12)

if not rows:
    logging.info("No season stats parsed — nothing to append")
    sys.exit(0)

fieldnames = ["snapshot_ts", "player_key", "stat_id", "stat_value"]
n = safe_write_csv(OUT, rows, fieldnames, mode="a")
logging.info("Appended %d rows to %s", n, OUT)
