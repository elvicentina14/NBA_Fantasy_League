# yahoo_utils.py
def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def unwrap(node):
    """
    Yahoo JSON returns:
    - dict
    - list
    - int (!!!)
    This normalizes everything to dict or list safely.
    """
    if isinstance(node, dict):
        return node
    if isinstance(node, list):
        return node
    return {}


def extract_players(container):
    """
    Handles:
    players: { 0: {...}, 1: {...} }
    players: [ {...}, {...} ]
    """
    out = []
    if isinstance(container, dict):
        for _, v in container.items():
            out.append(v)
    elif isinstance(container, list):
        out.extend(container)
    return out
