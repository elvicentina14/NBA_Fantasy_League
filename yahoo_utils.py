# yahoo_utils.py
"""Utilities to safely unwrap Yahoo Fantasy JSON shapes (lists of fragments)."""

from typing import Any, Dict, List


def as_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def first_dict(x: Any) -> Dict:
    """Return the first dict found inside x (which may be a list)."""
    if isinstance(x, list):
        for item in x:
            if isinstance(item, dict):
                return item
        return {}
    return x if isinstance(x, dict) else {}


def find_all(obj: Any, key: str) -> List[Any]:
    """
    Recursively find all values for `key` in nested dict/list structure.
    Returns list of found values (may be empty).
    """
    out = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                out.append(v)
            else:
                out.extend(find_all(v, key))
    elif isinstance(obj, list):
        for item in obj:
            out.extend(find_all(item, key))
    return out
