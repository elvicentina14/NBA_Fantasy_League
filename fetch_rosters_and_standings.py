#!/usr/bin/env python3
"""
fetch_rosters_and_standings.py

Fetches team rosters and standings from Yahoo Fantasy for a given LEAGUE_KEY.
Robust to Yahoo's changing JSON shapes: tries multiple league endpoints and
recursively searches for team/player nodes.

Outputs:
 - team_rosters.csv   (team_key, team_name, player_key, player_name, position)
 - standings.csv      (team_key, team_name, rank, wins, losses, ties, pct)
"""
from yahoo_oauth import OAuth2
import os
import pandas as pd
from typing import Any, Dict, List

CONFIG_FILE = "oauth2.json"
LEAGUE_KEY = os.environ.get("LEAGUE_KEY")

if not LEAGUE_KEY:
    raise SystemExit("LEAGUE_KEY environment variable is not set")

# --------- helpers --------- #

def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

def get_json(oauth: OAuth2, path: str) -> Dict[str, Any]:
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + path
    if "format=json" not in url:
        url += ("&" if "?" in url else "?") + "format=json"
    resp = oauth.session.get(url)
    # If Yahoo returns a non-JSON 999 page this will raise or be non-200
    try:
        resp.raise_for_status()
    except Exception:
        print(f"Non-200 from {url}: {resp.status_code if resp is not None else 'n/a'}")
        return {}
    try:
        return resp.json()
    except Exception:
        print(f"JSON decode error for {url}")
        return {}

def find_all_teams(obj: Any, out: List[Dict[str, Any]]):
    """
    Recursively find dicts that look like team objects (heuristic: has team_key).
    """
    if isinstance(obj, dict):
        if "team_key" in obj:
            out.append(obj)
        for v in obj.values():
            find_all_teams(v, out)
    elif isinstance(obj, list):
        for item in obj:
            find_all_teams(item, out)

def find_all_players(obj: Any, out: List[Dict[str, Any]]):
    """
    Recursively find dicts that look like player objects (heuristic: has player_key).
    """
    if isinstance(obj, dict):
        if "player_key" in obj:
            out.append(obj)
        for v in obj.values():
            find_all_players(v, out)
    elif isinstance(obj, list):
        for item in obj:
            find_all_players(item, out)

def extract_name_from_node(node: Dict[str,Any]) -> str:
    # Yahoo sometimes stores name as {"full": "..."} or more nested shapes
    if not isinstance(node, dict):
        return str(node)
    name = node.get("name") or node.get("full") or {}
    if isinstance(name, dict):
        return name.get("full") or name.get("first", "") + " " + name.get("last", "")
    return str(name)

# --------- main --------- #

def main():
    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"{CONFIG_FILE} missing (must contain oauth2 data)")

    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token invalid. Refresh locally and update oauth2.json secret.")

    # Try multiple league endpoints to maximize chance of finding teams
    tried_paths = [
        f"league/{LEAGUE_KEY}/teams",
        f"league/{LEAGUE_KEY}/standings",
        f"league/{LEAGUE_KEY}/scoreboard",
        f"league/{LEAGUE_KEY}",
        f"league/{LEAGUE_KEY}/teams;out=roster",
    ]

    team_nodes: List[Dict[str,Any]] = []
    for p in tried_paths:
        data = get_json(oauth, p)
        if not data:
            continue
        find_all_teams(data, team_nodes)
        if team_nodes:
            print(f"Found {len(team_nodes)} team nodes using endpoint: {p}")
            break

    if not team_nodes:
        raise SystemExit("❌ Yahoo returned no teams (tried multiple endpoints).")

    # Deduplicate teams by team_key
    dedup = {}
    for t in team_nodes:
        tk = t.get("team_key")
        if not tk:
            continue
        if tk in dedup:
            # prefer to keep the one that has name or standings
            if not dedup[tk].get("name") and t.get("name"):
                dedup[tk] = t
        else:
            dedup[tk] = t

    teams = list(dedup.values())
    print(f"Using {len(teams)} unique teams.")

    # Build rosters
    roster_rows = []
    for t in teams:
        team_key = t.get("team_key")
        team_name = extract_name_from_node(t.get("name") or t.get("team_name") or t)

        print(f"→ Fetch roster for {team_key} / {team_name}")

        # roster endpoint is reliable for players; if it fails we fall back to searching the team node
        roster_data = get_json(oauth, f"team/{team_key}/roster")
        players: List[Dict[str,Any]] = []
        if roster_data:
            find_all_players(roster_data, players)

        # fallback: look inside the team node we already found
        if not players:
            find_all_players(t, players)

        # Dedup players
        seen_p = set()
        for p in players:
            pk = p.get("player_key")
            if not pk or pk in seen_p:
                continue
            seen_p.add(pk)
            pname = extract_name_from_node(p.get("name") or p.get("full") or p)
            pos = p.get("display_position") or p.get("position") or p.get("selected_position") or ""
            roster_rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": pk,
                "player_name": pname,
                "position": pos
            })

    if roster_rows:
        df_rosters = pd.DataFrame(roster_rows)
        df_rosters = df_rosters.drop_duplicates(subset=["team_key","player_key"])
        df_rosters.to_csv("team_rosters.csv", index=False)
        print(f"Wrote {len(df_rosters)} rows → team_rosters.csv")
    else:
        print("No roster rows built; team_rosters.csv will not be created.")

    # Build standings: try to extract from team node first, else call /standings endpoint
    standings_rows = []
    for t in teams:
        tk = t.get("team_key")
        tname = extract_name_from_node(t.get("name") or t.get("team_name") or t)
        # try common fields
        ts = t.get("team_standings") or t.get("standings") or {}
        # outcome_totals might be nested
        ot = ts.get("outcome_totals") if isinstance(ts, dict) else {}
        standings_rows.append({
            "team_key": tk,
            "team_name": tname,
            "rank": ts.get("rank") or ts.get("rank_full") or None,
            "wins": ot.get("wins"),
            "losses": ot.get("losses"),
            "ties": ot.get("ties"),
            "pct": ot.get("percentage") or ts.get("percentage") or None
        })

    # If the collected standings look empty, fallback to explicit standings endpoint
    non_empty = [r for r in standings_rows if any([r.get("rank"), r.get("wins"), r.get("losses"), r.get("pct")])]
    if not non_empty:
        print("Attempting explicit standings endpoint fallback...")
        data = get_json(oauth, f"league/{LEAGUE_KEY}/standings")
        if data:
            teams2 = []
            find_all_teams(data, teams2)
            for t in teams2:
                tk = t.get("team_key")
                tname = extract_name_from_node(t.get("name") or t)
                ts = t.get("team_standings") or {}
                ot = ts.get("outcome_totals") if isinstance(ts, dict) else {}
                standings_rows.append({
                    "team_key": tk,
                    "team_name": tname,
                    "rank": ts.get("rank"),
                    "wins": ot.get("wins"),
                    "losses": ot.get("losses"),
                    "ties": ot.get("ties"),
                    "pct": ot.get("percentage") or ts.get("percentage") or None
                })

    if standings_rows:
        df_stand = pd.DataFrame(standings_rows)
        df_stand = df_stand.drop_duplicates(subset=["team_key"])
        df_stand.to_csv("standings.csv", index=False)
        print(f"Wrote {len(df_stand)} rows → standings.csv")
    else:
        print("No standings rows found; standings.csv will not be created.")

if __name__ == "__main__":
    main()
