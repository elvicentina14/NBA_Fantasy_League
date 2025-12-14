# yahoo_utils.py

def merge_kv_list(items):
    """
    Yahoo returns objects as:
    [
      {"player_key": "..."},
      {"name": {"full": "..."}},
      ...
    ]

    This function:
    - Accepts list | dict | None
    - ALWAYS returns a dict
    """
    if items is None:
        return {}

    if isinstance(items, dict):
        return items

    merged = {}
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                merged.update(item)
    return merged


def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]
