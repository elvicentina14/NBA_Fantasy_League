
#!/usr/bin/env python3
"""
Fetch Yahoo NBA Fantasy league data and export CSVs.

Outputs under: data/YYYY-MM-DD/
- league_settings.csv
- league_standings.csv
- league_teams.csv
- league_rosters.csv
- players_raw.csv
- player_stats.csv
- stat_definitions.csv

Relies on documented 2025 methods:
  League.settings(), League.standings(), League.teams(),
  League.roster(team_key), League.players(start,count),
  League.player_stats(player_key, season|week),
  League.stat_categories()
Docs: https://yahoo-fantasy-api.readthedocs.io/en/latest/yahoo_fantasy_api.html
Package: https://pypi.org/project/yahoo-fantasy-api/
Yahoo API guide: https://developer.yahoo.com/fantasysports/guide/
"""
import os, sys, time
from datetime import datetime
from dateutil.tz import tzlocal
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt

from yahoo_fantasy_api import game as yf_game
from yahoo_fantasy_api import league as yf_league
from yahoo_fantasy_api import yhandler  # OAuth2 handler used by the library

# --- Read config from env (provided by GitHub Actions) ---
CLIENT_ID = os.environ.get("YAHOO_CLIENT_ID")
CLIENT_SECRET = os.environ.get("YAHOO_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("YAHOO_REFRESH_TOKEN")
LEAGUE_KEY = os.environ.get("YAHOO_LEAGUE_KEY", "").strip()  # e.g., "nba.l.123456"
STAT_SEASON = os.environ.get("STAT_SEASON")  # e.g., "2025"
STAT_WEEK = os.environ.get("STAT_WEEK")      # e.g., "9" (optional)

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, LEAGUE_KEY]):
    print("Missing required env vars. Set YAHOO_CLIENT_ID, YAHOO_CLIENT_SECRET, "
          "YAHOO_REFRESH_TOKEN, YAHOO_LEAGUE_KEY", file=sys.stderr)
    sys.exit(1)

# --- Minimal OAuth handler (env-based; no token files) ---
class EnvOAuth(yhandler.OAuth2):
    """
    Headless OAuth2 for automation: uses refresh token to obtain/refresh access token.
    """
    def __init__(self, client_id, client_secret, refresh_token):
        super().__init__(None, None, from_file=False, access_token=None, refresh_token=refresh_token)
        self.consumer_key = client_id
        self.consumer_secret = client_secret
        self.token = {
            "access_token": None,
            "refresh_token": refresh_token,
            "token_time": 0,
            "token_type": "bearer",
            "expires_in": 3600,
            "client_id": client_id,
            "client_secret": client_secret
        }
    def refresh_access_token(self):
        # Delegate to library's refresh implementation
        return super().refresh_access_token()

oauth = EnvOAuth(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def get_league():
    return yf_league.League(oauth, LEAGUE_KEY)

@retry(wait=wait_exponential(multiplier=1, min=2, max=30), stop=stop_after_attempt(5))
def get_game():
    return yf_game.Game(oauth, "nba")

league = get_league()
game = get_game()

# --- Output folder (dated) ---
today = datetime.now(tzlocal()).strftime("%Y-%m-%d")
out_dir = os.path.join("data", today)
os.makedirs(out_dir, exist_ok=True)

def save_csv(df, name):
    path = os.path.join(out_dir, f"{name}.csv")
    df.to_csv(path, index=False)
    print(f"Saved: {path}")

# --- 1) League settings + standings (rankings) ---
settings = league.settings()      # documented in 2025 docs
standings = league.standings()    # documented in 2025 docs

pd.json_normalize(settings).to_csv(os.path.join(out_dir, "league_settings.csv"), index=False)
pd.DataFrame(standings).to_csv(os.path.join(out_dir, "league_standings.csv"), index=False)

# --- 2) Teams + rosters ---
teams = league.teams()            # documented
pd.DataFrame(teams).to_csv(os.path.join(out_dir, "league_teams.csv"), index=False)

roster_rows = []
for t in teams:
    team_key = t.get("team_key")
    team_id = t.get("team_id")
    team_name = t.get("name")
    try:
        roster = league.roster(team_key)   # documented
        for p in roster:
            roster_rows.append({
                "team_id": team_id,
                "team_name": team_name,
                "team_key": team_key,
                "player_id": p.get("player_id"),
                "player_key": p.get("player_key"),
                "name": (p.get("name", {}) or {}).get("full"),
                "editorial_team_abbr": p.get("editorial_team_abbr"),
                "eligible_positions": ",".join(p.get("eligible_positions", []) or []),
                "selected_position": (p.get("selected_position", {}) or {}).get("position", ""),
                "status": p.get("status") or "",
                "is_undroppable": p.get("is_undroppable"),
            })
    except Exception as e:
        print(f"Roster failed for {team_key}: {e}", file=sys.stderr)
        time.sleep(1)

pd.DataFrame(roster_rows).to_csv(os.path.join(out_dir, "league_rosters.csv"), index=False)

# --- 3) Player pool + stats (season or week) ---
players_all = []
start = 0
count = 25
while True:
    try:
        chunk = league.players(start=start, count=count)   # documented paging
        if not chunk:
            break
        players_all.extend(chunk)
        start += count
    except Exception as e:
        print(f"Players paging failed at start={start}: {e}", file=sys.stderr)
        break

df_players = pd.DataFrame(players_all)
df_players.to_csv(os.path.join(out_dir, "players_raw.csv"), index=False)

def fetch_stats(player_key: str):
    # League.player_stats supports week or season in current docs
    if STAT_WEEK:
        return league.player_stats(player_key, week=int(STAT_WEEK))
    elif STAT_SEASON:
        return league.player_stats(player_key, season=STAT_SEASON)
    else:
        return league.player_stats(player_key)  # default (current season context)

stat_rows = []
for _, row in df_players.iterrows():
    pkey = row.get("player_key")
    pid = row.get("player_id")
    name = (row.get("name") or {}).get("full") if isinstance(row.get("name"), dict) else row.get("name")
    try:
        stats = fetch_stats(pkey)
        flat = {f"stat_{s['stat_id']}": s.get("value") for s in stats}
        base = {
            "player_id": pid,
            "player_key": pkey,
            "name": name,
            "editorial_team_abbr": row.get("editorial_team_abbr"),
            "display_position": row.get("display_position"),
            "status": row.get("status"),
            "is_undroppable": row.get("is_undroppable"),
        }
        base.update(flat)
        stat_rows.append(base)
    except Exception as e:
        print(f"Stats failed for {pkey} ({name}): {e}", file=sys.stderr)
        continue

pd.DataFrame(stat_rows).to_csv(os.path.join(out_dir, "player_stats.csv"), index=False)

# --- 4) Stat definitions (ID -> readable names) ---
stat_defs = league.stat_categories()     # documented
pd.DataFrame(stat_defs).to_csv(os.path.join(out_dir, "stat_definitions.csv"), index=False)

print("Yahoo NBA extraction complete.")
