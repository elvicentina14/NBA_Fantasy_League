# http_helpers.py
import time
import logging

def safe_get(session, url, max_retries=3, backoff=0.5, timeout=30):
    """
    session: requests-like session (oauth.session)
    Returns (status_code, json) or raises.
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 200:
                try:
                    return 200, r.json()
                except Exception as e:
                    logging.exception("Failed to decode JSON")
                    raise
            else:
                logging.warning("HTTP %s %s (attempt %d/%d)", r.status_code, url, attempt, max_retries)
                last_exc = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_exc = e
            logging.warning("Request error %s (attempt %d/%d) %s", e, attempt, max_retries, url)
        time.sleep(backoff * attempt)
    raise last_exc
