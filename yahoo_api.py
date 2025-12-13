import requests

BASE = "https://fantasysports.yahooapis.com/fantasy/v2"

def yahoo_get(oauth, path):
    if "format=json" not in path:
        sep = "&" if "?" in path else "?"
        path += f"{sep}format=json"

    url = f"{BASE}/{path}"
    r = oauth.session.get(url)
    r.raise_for_status()
    return r.json()["fantasy_content"]
