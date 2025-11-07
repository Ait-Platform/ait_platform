# utils/routing.py
from werkzeug.routing import BuildError
from flask import (
    Blueprint, current_app, render_template,
    redirect, url_for, request, flash,
    session, abort
    )
# Centralized mapping of roles -> ordered list of possible endpoints.
# app/utils/routing.py

__all__ = ["pick_dashboard", "get_dashboard_route", "get_dashboard_endpoint"]

# ONE source of truth: ordered candidates per role/subject.
# Each tuple: (endpoint_name, needs_subject_bool)
DASHBOARD_MAP = {
    # Admins: prefer tiles; fall back to subject dashboard; then legacy admin home; finally public welcome
    "admin": [
        ("admin_bp.index", False),             # same path, alternate endpoint
        ("admin_bp.admin_home", False),        # 4-tile dashboard FIRST
        ("admin_bp.subject_dashboard", True),  # /admin/<subject>/ fallback
        ("public_bp.welcome", False),          # last resort
    ],

    # Subject-specific roles (tweak to your app)
    "reading_admin": [
        ("admin_bp.subject_dashboard", True),
        ("admin_bp.admin_home", False),
        ("public_bp.welcome", False),
    ],
    "reading_learner": [
        ("public_bp.reading_home", False),
        ("public_bp.welcome", False),
    ],

    # Generic subject fallback (when role collapses to subject like "reading")
    "reading": [
        ("public_bp.reading_home", False),
        ("public_bp.welcome", False),
    ],
}

def pick_dashboard(role: str | None, subject: str | None = None) -> tuple[str, dict]:
    """
    Preferred helper: returns (endpoint, kwargs) for the first resolvable candidate.
    Use like: ep, kw = pick_dashboard(role, subject); redirect(url_for(ep, **kw))
    """
    role = (role or "").strip().lower()
    subject = (subject or "").strip().lower() or "reading"

    candidates = DASHBOARD_MAP.get(role)
    if not candidates:
        # If role looks like "<subject>_something", try the base subject.
        base = role.split("_", 1)[0] if "_" in role else (role or subject)
        candidates = DASHBOARD_MAP.get(base, [])
    if not candidates:
        candidates = [("public_bp.welcome", False)]

    for ep, needs_subject in candidates:
        try:
            kw = {"subject": subject} if needs_subject else {}
            url_for(ep, **kw)  # probe build
            return ep, kw
        except BuildError:
            continue

    current_app.logger.warning("pick_dashboard: no endpoint resolved; using public welcome")
    return "public_bp.welcome", {}

def get_dashboard_route(role: str | None, subject: str | None = None) -> str:
    """
    Back-compat helper for old call sites that expect an endpoint NAME requiring NO kwargs.
    We reuse the same DASHBOARD_MAP but skip candidates that need 'subject'.
    Use like: ep = get_dashboard_route(role, subject); redirect(url_for(ep))
    """
    role = (role or "").strip().lower()
    subject = (subject or "").strip().lower() or "reading"

    # Try exact role first, filtering out endpoints that need kwargs
    filtered = [(ep, needs) for ep, needs in DASHBOARD_MAP.get(role, []) if not needs]
    # Then fall back to subject key
    if not filtered:
        base = role.split("_", 1)[0] if "_" in role else (role or subject)
        filtered = [(ep, needs) for ep, needs in DASHBOARD_MAP.get(base, []) if not needs]
    if not filtered:
        filtered = [("public_bp.welcome", False)]

    for ep, _ in filtered:
        try:
            url_for(ep)  # probe
            return ep
        except BuildError:
            continue

    current_app.logger.warning("get_dashboard_route: no endpoint resolved; using public welcome")
    return "public_bp.welcome"

# Alias for any older imports that still reference this name
def get_dashboard_endpoint(role: str | None, subject: str | None = None) -> str:
    return get_dashboard_route(role, subject)
