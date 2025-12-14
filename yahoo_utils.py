def list_to_dict(obj):
    """
    Yahoo Fantasy wraps objects as:
    [ { "key": "value" }, { "key2": "value2" } ]

    This converts:
      list -> dict
      dict -> dict
      None -> {}
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


def safe_get(d, key, default=None):
    """
    Safe getter that works even if d is None
    """
    if not isinstance(d, dict):
        return default
    return d.get(key, default)
