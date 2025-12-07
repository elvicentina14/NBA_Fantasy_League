# fetch_full_player_stats.py
from yahoo_oauth import OAuth2
import os
import json
import pandas as pd

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


def extract_stats_for_player(oauth, player_key):
    """
    Call player/{player_key}/stats and return a list of dicts:
    {
      player_key, player_name, coverage, period, stat_id, stat_value
    }
    """
    rel = f"player/{player_key}/stats"
    data = get_json(oauth, rel)

    fc = data.get("fantasy_content", {})
    player_node = fc.get("player")

    # player_node is usually a list like:
    # [ {player_key}, {...}, {name: {...}}, ..., {player_stats: {...}} ]
    # but we keep it generic and search for name + player_stats
    if player_node is None:
        return []

    player_name = None
    if isinstance(player_node, list):
        name_obj = find_first(player_node, "name")
    else:
        name_obj = find_first(player_node, "name")

    if isinstance(name_obj, dict) and "full" in name_obj:
        player_name = name_obj["full"]
    else:
        player_name = find_first(name_obj, "full") if isinstance(name_obj, dict) else None

    if not player_name:
        player_name = "Unknown"

    player_stats = find_first(player_node, "player_stats")
    if not isinstance(player_stats, dict):
        return []

    coverage_type = player_stats.get("coverage_type") or find_first(player_stats, "coverage_type")
    period = (
        player_stats.get("season")
        or player_stats.get("week")
        or player_stats.get("date")
        or find_first(player_stats, "season")
        or find_first(player_stats, "week")
        or find_first(player_stats, "date")
    )

    stats_node = player_stats.get("stats") or find_first(player_stats, "stats")
    if stats_node is None:
        return []

    # stats_node is usually {"stat": [ {...}, {...} ]}
    stat_items = []
    if isinstance(stats_node, dict):
        if "stat" in stats_node:
            stat_items = ensure_list(stats_node["stat"])
        else:
            stat_items = [stats_node]
    elif isinstance(stats_node, list):
        stat_items = stats_node

    rows = []
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

        rows.append(
            {
                "player_key": str(player_key),
                "player_name": player_name,
                "coverage": coverage_type,
                "period": period,
                "stat_id": str(stat_id),
                "stat_value": value,
            }
        )

    return rows


def main():
    # 1) OAuth (re-use same oauth2.json you already have)
    oauth = OAuth2(None, None, from_file=CONFIG_FILE)
    if not oauth.token_is_valid():
        raise SystemExit("OAuth token is not valid inside GitHub Actions – refresh locally first.")

    # 2) league_players.csv must already exist (from fetch_players_and_stats.py)
    if not os.path.exists("league_players.csv"):
        raise SystemExit("league_players.csv not found. Run fetch_players_and_stats.py first.")

    league_players = pd.read_csv("league_players.csv", dtype=str)
    if "player_key" not in league_players.columns:
        raise SystemExit("league_players.csv missing 'player_key' column.")

    player_keys = sorted(league_players["player_key"].dropna().unique().tolist())
    print(f"Found {len(player_keys)} unique player_keys in league_players.csv")

    all_rows = []

    for idx, pk in enumerate(player_keys, start=1):
        print(f"Fetching stats for player {idx}/{len(player_keys)}: {pk}")
        try:
            rows = extract_stats_for_player(oauth, pk)
        except Exception as e:
            print(f"Failed to fetch stats for {pk}: {type(e).__name__} {e}")
            rows = []
        all_rows.extend(rows)

    # 3) Build player_stats_full.csv
    if not all_rows:
        print("WARNING: No stats found for any player.")
        df_stats = pd.DataFrame(
            columns=["player_key", "player_name", "coverage", "period", "stat_id", "stat_value"]
        )
    else:
        df_stats = pd.DataFrame(all_rows)

    df_stats.to_csv("player_stats_full.csv", index=False)
    print(f"Saved {len(df_stats)} rows to player_stats_full.csv")

    # 4) Join with league_players to create the combined view
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
