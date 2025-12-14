# yahoo_utils.py

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def first_dict(lst):
    for item in as_list(lst):
        if isinstance(item, dict):
            return item
    return None


def find_all(node, key):
    """
    Recursively find ALL values for key inside Yahoo-style list/dict trees
    """
    found = []

    if isinstance(node, dict):
        for k, v in node.items():
            if k == key:
                found.extend(as_list(v))
            else:
                found.extend(find_all(v, key))

    elif isinstance(node, list):
        for item in node:
            found.extend(find_all(item, key))

    return found


def extract_fragment(lst, key):
    """
    Scan list[dict] and return dict[key] if present
    """
    for frag in as_list(lst):
        if isinstance(frag, dict) and key in frag:
            return frag[key]
    return None


def extract_name(lst):
    """
    Extract name.full safely
    """
    for frag in as_list(lst):
        if isinstance(frag, dict) and "name" in frag:
            name_block = frag["name"]
            if isinstance(name_block, list) and name_block:
                return name_block[0].get("full")
    return None
