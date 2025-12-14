def ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def unwrap(obj):
    """
    Yahoo sometimes wraps objects as:
    [int, {actual_data}]
    """
    if isinstance(obj, list):
        if len(obj) == 2 and isinstance(obj[0], int):
            return obj[1]
        return obj[0]
    return obj
