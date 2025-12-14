# yahoo_normalize.py

def first(obj):
    """
    Yahoo Fantasy API normalizer.
    Always returns a dict (or {}).
    Safely unwraps list -> dict -> list chains.
    """
    while isinstance(obj, list):
        if not obj:
            return {}
        obj = obj[0]
    return obj if isinstance(obj, dict) else {}
