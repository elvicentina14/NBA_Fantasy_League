# yahoo_utils.py

def as_list(x):
    return x if isinstance(x, list) else []

def first_dict(x):
    """
    Return first dict inside list or dict itself.
    Never throws.
    """
    if isinstance(x, list):
        for i in x:
            if isinstance(i, dict):
                return i
        return {}
    return x if isinstance(x, dict) else {}
