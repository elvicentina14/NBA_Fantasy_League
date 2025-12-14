# yahoo_utils.py

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def first_dict(items):
    """
    Return the first dict found in a list.
    """
    for item in as_list(items):
        if isinstance(item, dict):
            return item
    return {}


def find_all_dicts(items):
    """
    Return all dicts found in a list.
    """
    return [x for x in as_list(items) if isinstance(x, dict)]


def safe_get(obj, *keys):
    """
    Safely walk nested dict/list structures.
    """
    cur = obj
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k)
        elif isinstance(cur, list):
            try:
                cur = cur[k]
            except Exception:
                return None
        else:
            return None
    return cur
