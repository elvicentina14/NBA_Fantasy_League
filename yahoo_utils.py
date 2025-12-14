# yahoo_utils.py

def as_list(x):
    """
    Ensure Yahoo objects are iterable.
    """
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def list_to_dict(obj):
    """
    Convert Yahoo list-wrapped dict fragments into a single dict.
    """
    if obj is None:
        return {}

    if isinstance(obj, dict):
        return obj

    if isinstance(obj, list):
        out = {}
        for item in obj:
            if isinstance(item, dict):
                out.update(item)
        return out

    return {}


def first_dict(obj):
    """
    Return the first dict found in a list or dict structure.
    """
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                return item
    return {}


def find_all(obj, key):
    """
    Recursively find all values for a given key.
    """
    results = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                results.append(v)
            results.extend(find_all(v, key))

    elif isinstance(obj, list):
        for item in obj:
            results.extend(find_all(item, key))

    return results


def safe_get(d, key, default=None):
    """
    Safe dict get.
    """
    if not isinstance(d, dict):
        return default
    return d.get(key, default)
