def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def first_dict(items):
    for x in as_list(items):
        if isinstance(x, dict):
            return x
    return {}


def find_all(items, key):
    """
    Recursively find ALL dicts that contain `key`
    """
    found = []
    for x in as_list(items):
        if isinstance(x, dict):
            if key in x:
                found.extend(as_list(x[key]))
            for v in x.values():
                found.extend(find_all(v, key))
        elif isinstance(x, list):
            found.extend(find_all(x, key))
    return found
