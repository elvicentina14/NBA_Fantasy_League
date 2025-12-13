import os
import csv
import json
import requests
from yahoo_oauth import OAuth2

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")
OUT_FILE = "team_rosters.csv"


def ensure_list(x):
    if isinstance(x, list):
        return x
    return [x]


def main():
    if not LEAGUE_KEY:
        raise RuntimeError("LEAGUE_KEY env var not set")

    oauth = OAuth2(None, None, from_file="oauth2.json")

    url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/{LEAGUE_KEY}/teams/roster"
    resp = oauth.session.get(url, params={"format": "json"})
    resp.raise_for_status()

    data = resp.json()

    # Yahoo structure:
    # fantasy_content -> league -> [0, {teams: {...}}]
    league = data["fantasy_content"]["league"][1]
    teams_node = league["teams"]

    rows = []

    for _, team_wrapper in teams_node.items():
        if not isinstance(team_wrapper, dict):
            continue

        team = team_wrapper.get("team")
        if not team:
            continue

        team_key = team.get("team_key")
        team_name = team.get("name")

        roster = team.get("roster", {})
        players = roster.get("players", {})

        for _, player_wrapper in players.items():
            if not isinstance(player_wrapper, dict):
                continue

            player = player_wrapper.get("player")
            if not player:
                continue

            player_key = player.get("player_key")

            name = player.get("name", {})
            full_name = name.get("full")

            positions = player.get("eligible_positions", {}).get("position", [])
            if isinstance(positions, list):
                position = "/".join(positions)
            else:
                position = positions

            rows.append({
                "team_key": team_key,
                "team_name": team_name,
                "player_key": player_key,
                "player_name": full_name,
                "position": position
            })

    if not rows:
        raise RuntimeError("No roster data extracted — API returned unexpected structure")

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["team_key", "team_name", "player_key", "player_name", "position"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows → {OUT_FILE}")


if __name__ == "__main__":
    main()
