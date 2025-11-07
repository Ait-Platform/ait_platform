# app/utils/nav.py
from flask import request, url_for

def _try_url(endpoint: str):
    try:
        return url_for(endpoint)
    except Exception:
        return None

def resolve_next(subject: str | None):
    """
    Decide a sensible post-payment landing URL with ZERO hard-coding:
    1) caller-provided ?next=... wins
    2) try common endpoint patterns derived from the subject
    3) generic fallbacks
    """
    # 1) explicit next from querystring/form wins
    if request.args.get("next"):
        return request.args.get("next")

    s = (subject or "").strip().lower()
    candidates = []
    if s:
        # Common patterns most apps use; we just *try* them
        candidates += [
            f"{s}_bp.dashboard",
            f"{s}_bp.home",
            f"{s}_bp.index",
            f"{s}_bp.about_{s}",
        ]

    # Generic fallbacks (only used if none of the above exist)
    candidates += [
        "student_bp.home",
        "public_bp.welcome",
    ]

    for ep in candidates:
        url = _try_url(ep)
        if url:
            return url

    # last resort: referrer or root
    return request.referrer or "/"
