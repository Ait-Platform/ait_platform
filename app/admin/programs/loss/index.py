from flask import render_template, url_for
from werkzeug.routing import BuildError

def _safe_url(endpoint: str) -> str:
    try:
        return url_for(endpoint)
    except BuildError:
        return "#"  # keep the button disabled if route not present

