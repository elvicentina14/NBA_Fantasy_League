# safe_io.py
import csv
import json
import os
from typing import List, Dict

def safe_write_csv(path: str, rows: List[Dict], fieldnames: List[str], mode: str = "w"):
    """
    Write rows safely. If rows empty, skip and return 0.
    mode: "w" or "a"
    """
    if not rows:
        return 0
    write_header = mode == "w" or not os.path.exists(path)
    with open(path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)

def debug_dump(obj, fname: str):
    # Dump compact JSON for CI inspection when DEBUG_DUMP=1
    try:
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
    except Exception:
        pass
