import os
import sys
import time
import csv
import logging
from datetime import datetime, timezone

import pandas as pd
from yahoo_oauth import OAuth2
from yahoo_utils import as_list, first_dict, find_all
from http_helpers import safe_get

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
if not LEAGUE_KEY:
    logging.error("LEAGUE_KEY env var not set")
    sys.exit(2)

PLAYERS_CSV = "league_players.csv"

SNAPSHOT_TS = datetime.now(timezone.utc)
SNAPSHOT_DATE = SNAPSHOT_TS.date().isoformat()

OUT_DIR = "data/snapshots"
OUT_FILE = os.path.join(
    OUT_DIR,
    f"fact_player_season_snapshot_{SNAPSHOT_DATE}.parquet"
)

os.makedirs(OUT_DIR, exist_ok=True)

oauth = OAuth2(None, None, from_file="oauth2.json")
session = oauth.session

# ---------------- Load players ----------------
if not os.path.exists(PLAYERS_CSV):
    logging.error("%s not found, run fetch_players.py first", PLAYERS_CSV)
    sys.exit(1)

players = []
with open(PLAYERS_CSV, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        if r.get("player_key"):
            players.append(r)

if not players:
    logging.info("No players found — exiting")
    sys.exit(0)

rows = []

# ---------------- Fetch stats ----------------
for idx, p in enumerate(players, start=1):
    pk = p["player_key"]
    logging.info("[%d/%d] Fetching stats for %s", idx, len(players), pk)

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/player/{pk}/stats?format=json"

    try:
        _, data = safe_get(session, url)
    except Exception:
        logging.exception("Failed to fetch stats for %s", pk)
        continue

    player_nodes = as_list(data.get("fantasy_content", {}).get("player"))
    if len(player_nodes) < 2:
        continue

    stats_frag = first_dict(player_nodes[1]).get("player_stats")
    stat_lists = find_all(stats_frag, "stat")

    for sl in stat_lists:
        for stat_item in (sl if isinstance(sl, list) else [sl]):
            s = first_dict(stat_item)
            if s.get("stat_id") is None:
                continue

            rows.append({
                "snapshot_ts": SNAPSHOT_TS,
                "snapshot_date": SNAPSHOT_DATE,
                "player_key": pk,
                "stat_id": int(s["stat_id"]),
                "stat_value": s.get("value")
            })

    time.sleep(0.12)

if not rows:
    logging.info("No stats collected")
    sys.exit(0)

new_df = pd.DataFrame(rows)

# ---------------- Enforce schema ----------------
new_df["snapshot_ts"] = pd.to_datetime(new_df["snapshot_ts"], utc=True)
new_df["snapshot_date"] = new_df["snapshot_date"].astype("string")
new_df["player_key"] = new_df["player_key"].astype("string")
new_df["stat_id"] = new_df["stat_id"].astype("int32")
new_df["stat_value"] = new_df["stat_value"].astype("string")

# ---------------- Load existing Parquet (if any) ----------------
if os.path.exists(OUT_FILE):
    logging.info("Existing snapshot found — merging and deduping")
    existing_df = pd.read_parquet(OUT_FILE)

    df = pd.concat([existing_df, new_df], ignore_index=True)
else:
    df = new_df

# ---------------- DEDUPE ----------------
# Keep the latest snapshot per (date, player, stat)
before = len(df)

df = (
    df.sort_values("snapshot_ts")
      .drop_duplicates(
          subset=["snapshot_date", "player_key", "stat_id"],
          keep="last"
      )
)

after = len(df)

logging.info(
    "Deduped snapshot: %d → %d rows (removed %d)",
    before, after, before - after
)

# ---------------- Write Parquet ----------------
df.to_parquet(
    OUT_FILE,
    engine="pyarrow",
    compression="snappy",
    index=False
)

logging.info("Wrote %d rows → %s", len(df), OUT_FILE)
