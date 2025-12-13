#!/usr/bin/env python3
"""
fetch_rosters_and_standings.py

Fetches team rosters and standings from Yahoo Fantasy (NBA).
Handles Yahoo's inconsistent formats safely.
Writes:
 - team_rosters.csv
 - standings.csv
"""

from yahoo_oauth import OAuth2
import pandas as pd
import os
from typing import Any, Dict, List

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

if not LEAGUE_KEY:
    raise SystemExit("LEAGUE_KEY env var not set")


# ---------- HELPERS ---------- #

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def deep_collect(obj: Any, predicate):
    """
    Recursively collect objects matching predicate (e.g., dicts with some key).
    """
    results = []
    if isinstance(obj, dict):
        if predicate(obj):
            results.append(obj)
        for v in obj.values():
            results.extend(deep_collect(v, predicate))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(deep_collect(item, predicate))
    return results


def get_json(oauth: OAuth2, path: str) -> Dict[str, Any]:
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += "?format=json"
    resp = oauth.session.get(url)
    try:
        resp.raise_for_status()
    except Exception:
        print(f"WARNING: Non-200 for {url}: {resp.status_code}")
        return {}
    try:
        return resp.json()
    except Exception:
        print(f"WARNING: JSON decode failed for {url}")
        return {}


def extract_name(o: Dict[str, Any]):
    """
    Safely extract a full name from Yahoo node.
    """
    if not isinstance(o, dict):
        return None
    n = o.get("name")
    if isinstance(n, dict):
        return n.get("full") or n.get("first") or n.get("last")
    return n


# ---------- MAIN ---------- #

def main():
    if not os.path.exists(CONFIG_FILE):
        raise SystemExit("oauth2.json not found")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid")

    # --- FETCH LEAGUE DATA (standings endpoint is most reliable) --- #
    print("Fetching league standings for team list...")
    data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
    if not data:
        raise SystemExit("Failed to fetch league standings")

    # collect all dicts that have team_key
    teams = deep_collect(data, lambda x: isinstance(x, dict) and "team_key" in x)

    if not teams:
        raise SystemExit("No teams found in Yahoo response")

    print(f"Found {len(teams)} teams")

    # --- WRITE STANDINGS --- #
    standings_rows = []
    for t in teams:
        tk = t.get("team_key")
        tname = extract_name(t)
        ts = t.get("team_standings") or {}
        ot = ts.get("outcome_totals") or {}

        standings_rows.append({
            "team_key": tk,
            "team_name": tname,
            "rank": ts.get("rank"),
            "wins": ot.get("wins"),
            "losses": ot.get("losses"),
            "ties": ot.get("ties"),
            "win_pct": ot.get("percentage")
        })

    df_stand = pd.DataFrame(standings_rows)
    df_stand = df_stand.drop_duplicates(subset=["team_key"])
    df_stand.to_csv("standings.csv", index=False)
    print(f"Saved {len(df_stand)} rows to standings.csv")

    # --- FETCH ROSTERS --- #
    roster_rows = []
    for t in df_stand["team_key"].dropna().unique().tolist():
        print(f"Fetching roster for {t}...")
        rd = get_json(oauth, f"team/{t}/roster")
        players = deep_collect(rd, lambda x: isinstance(x, dict) and "player_key" in x)
        for p in players:
            roster_rows.append({
                "team_key": t,
                "team_name": extract_name(p.get("editorial_team_full_name") or {}),
                "player_key": p.get("player_key"),
                "player_name": extract_name(p),
                "position": p.get("display_position") or p.get("primary_position")
            })

    df_roster = pd.DataFrame(roster_rows).drop_duplicates(subset=["team_key","player_key"])
    df_roster.to_csv("team_rosters.csv", index=False)
    print(f"Saved {len(df_roster)} rows to team_rosters.csv")


if __name__ == "__main__":
    main()
