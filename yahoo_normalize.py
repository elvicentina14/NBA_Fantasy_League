def first_dict(obj):
    """
    Yahoo Fantasy API normalizer.
    Always returns a dict or {}.
    """
    if isinstance(obj, list):
        if len(obj) == 0:
            return {}
        return first_dict(obj[0])
    if isinstance(obj, dict):
        return obj
    return {}
