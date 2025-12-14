# yahoo_helpers.py

def safe_items(node):
    """Iterate Yahoo dicts that mix numeric keys and 'count'."""
    if not isinstance(node, dict):
        return []
    return [(k, v) for k, v in node.items() if k != "count"]


def flatten_list(x):
    """Flatten nested Yahoo lists safely."""
    out = []
    if isinstance(x, list):
        for i in x:
            if isinstance(i, list):
                out.extend(i)
            else:
                out.append(i)
    return out


def extract_name(item):
    """Extract full player name from Yahoo name structures."""
    if not isinstance(item, dict):
        return None
    nm = item.get("name")
    if isinstance(nm, dict):
        return nm.get("full")
    return nm


def canonical_player_key(player_key, player_id):
    """
    Decide the best canonical key.
    Yahoo sometimes returns:
      - '466.p.4912'
      - '4912'
      - editorial keys
    """
    if player_key:
        return str(player_key)
    if player_id:
        return str(player_id)
    return None
