# yahoo_utils.py

def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def merge_fragments(fragments):
    """
    Yahoo 'player' or 'team' nodes are lists of dict fragments.
    This merges them into a single dict safely.
    """
    out = {}
    for frag in fragments:
        if isinstance(frag, dict):
            out.update(frag)
    return out


def iter_indexed_dict(d):
    """
    Yahoo containers look like:
      { "0": {...}, "1": {...}, "count": 14 }
    This yields only numeric keys.
    """
    if not isinstance(d, dict):
        return []
    return [v for k, v in d.items() if k.isdigit()]
