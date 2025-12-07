# fetch_full_player_stats.py
from yahoo_oauth import OAuth2
import os
import json
import pandas as pd

LEAGUE_KEY = os.environ.get("LEAGUE_KEY")  # comes from GitHub Secret

CONFIG_FILE = "oauth2.json"


def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def find_first(obj, key):
    """Recursively find the first value for a given key in nested dict/list."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = find_first(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_first(item, key)
            if found is not None:
                return found
    return None


def iter_player_blocks(obj):
    """
    Recursively yield 'player' blocks from the Yahoo JSON.
    A 'player' block is whatever is under a 'player' key.
    """
    if isinstance(obj, dict):
        if "player" in obj:
            for p in ensure_list(obj["player"]):
                yield p
        for v in obj.values():
            for p in iter_player_blocks(v):
                yield p
    elif isinstance(obj, list):
        for item in obj:
            for p in iter_player_blocks(item):
                yield p


def get_json(oauth, relative_path):
    """Call Yahoo Fantasy API with ?format=json appended."""
    base = "https://fantasysports.yahooapis.com/fantasy/v2/"
    url = base + relative_path
    if "format=json" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}format=json"
    resp = oauth.session.get(url)
    resp.raise_for_status()
    return resp.json()


def main():
    if not LEAGUE_KEY:
        raise SystemExit("LEAGUE_KEY env var is not set")

    # 1) OAuth session
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid inside GitHub Actions – refresh locally first.")

    # 2) Load league_players.csv (already created by your other script)
    if not os.path.exists("league_players.csv"):
        raise SystemExit("league_players.csv not found. Run fetch_players_and_stats.py first.")

    league_players = pd.read_csv("league_players.csv", dtype=str)
    if "player_key" not in league_players.columns:
        raise SystemExit("league_players.csv missing 'player_key' column.")

    player_keys = sorted(league_players["player_key"].dropna().unique().tolist())
    print(f"Found {len(player_keys)} unique player_keys in league_players.csv")

    stats_rows = []

    # 3) Fetch stats in chunks of up to 25 players (Yahoo API style)
    chunk_size = 25
    for i in range(0, len(player_keys), chunk_size):
        chunk = player_keys[i:i + chunk_size]
        keys_param = ",".join(chunk)
        rel = f"league/{LEAGUE_KEY}/players;player_keys={keys_param}/stats"
        print(f"Fetching stats for players {i + 1}–{i + len(chunk)}")
        data = get_json(oauth, rel)

        # 4) Walk all 'player' blocks that contain player_stats
        seen_in_chunk = set()
        for p_block in iter_player_blocks(data):
            # p_block is usually a list like [ {player_key}, {player_id}, {name}, ..., {player_stats}, ... ]
            player_key = find_first(p_block, "player_key")
            if not player_key:
                continue

            # Avoid duplicates if any
            if (player_key, id(p_block)) in seen_in_chunk:
                continue
            seen_in_chunk.add((player_key, id(p_block)))

            name_obj = find_first(p_block, "name")
            if isinstance(name_obj, dict) and "full" in name_obj:
                player_name = name_obj["full"]
            else:
                player_name = find_first(p_block, "full") or "Unknown"

            player_stats = find_first(p_block, "player_stats")
            if not isinstance(player_stats, dict):
                continue

            coverage_type = player_stats.get("coverage_type") or find_first(player_stats, "coverage_type")
            period = (
                player_stats.get("season")
                or player_stats.get("week")
                or player_stats.get("date")
                or find_first(player_stats, "season")
                or find_first(player_stats, "week")
                or find_first(player_stats, "date")
            )

            # The actual stats live under something like player_stats["stats"]["stat"]
            stats_node = player_stats.get("stats") or find_first(player_stats, "stats")
            if stats_node is None:
                continue

            # stats_node might be:
            # - {"stat": [ {...}, {...} ]}
            # - [ {"stat": {...}}, {"stat": {...}} ]
            # - or directly a list of dicts with stat_id/value
            stat_items = []
            if isinstance(stats_node, dict):
                if "stat" in stats_node:
                    stat_items = ensure_list(stats_node["stat"])
                else:
                    stat_items = [stats_node]
            elif isinstance(stats_node, list):
                stat_items = stats_node
            else:
                stat_items = []

            for item in stat_items:
                # Sometimes wrapped in {"stat": {...}}
                if isinstance(item, dict) and "stat" in item:
                    s = item["stat"]
                else:
                    s = item

                stat_id = find_first(s, "stat_id")
                value = find_first(s, "value")

                if stat_id is None:
                    continue

                stats_rows.append(
                    {
                        "player_key": str(player_key),
                        "player_name": player_name,
                        "coverage": coverage_type,
                        "period": period,
                        "stat_id": str(stat_id),
                        "stat_value": value,
                    }
                )

    # 5) Build DataFrame of all stats
    if not stats_rows:
        print("WARNING: No stats found.")
        df_stats = pd.DataFrame(
            columns=["player_key", "player_name", "coverage", "period", "stat_id", "stat_value"]
        )
    else:
        df_stats = pd.DataFrame(stats_rows)

    df_stats.to_csv("player_stats_full.csv", index=False)
    print(f"Saved {len(df_stats)} rows to player_stats_full.csv")

    # 6) Combined player view: join with league_players on player_key (+ name for safety)
    # We keep all columns from league_players and add stats columns.
    combined = league_players.merge(
        df_stats,
        on=["player_key", "player_name"],
        how="left",
        validate="m:m"
    )

    combined.to_csv("combined_player_view_full.csv", index=False)
    print(f"Saved {len(combined)} rows to combined_player_view_full.csv")


if __name__ == "__main__":
    main()
