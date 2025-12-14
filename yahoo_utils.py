# yahoo_utils.py

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def merge_kv_list(block):
    """
    Yahoo pattern:
    [
      {"key1": value1},
      {"key2": value2}
    ]
    → {"key1": value1, "key2": value2}
    """
    out = {}
    for item in as_list(block):
        if isinstance(item, dict):
            out.update(item)
    return out
