# app/utils/roles.py
from __future__ import annotations
import time
from typing import Dict, Tuple, Optional
from flask import session
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db

# key -> (refresh_ts, alias_map, canonical_set, default_role)
_ROLE_CACHE: Dict[str, Tuple[float, dict, set, str]] = {}

def _load_role_maps() -> Tuple[dict, set, str]:
    """
    Loads:
      - alias_map: e.g. {'mentor':'tutor', 'teacher':'tutor'}
      - canonical_set: {'learner','tutor','parent','admin',...}
      - default_role: the single role with role.is_default=1 (fallback 'learner')
    """
    # Use .mappings() for predictable dict-like rows
    alias_rows = db.session.execute(text(
        "SELECT LOWER(TRIM(alias)) AS alias, LOWER(TRIM(canonical)) AS canonical FROM role_alias"
    )).mappings().all()

    role_rows = db.session.execute(text(
        "SELECT LOWER(TRIM(name)) AS name, COALESCE(is_default,0) AS is_default FROM role"
    )).mappings().all()

    alias_map = {r["alias"]: r["canonical"] for r in alias_rows if r.get("alias") and r.get("canonical")}
    canonical_set = {r["name"] for r in role_rows if r.get("name")}
    default_role = next((r["name"] for r in role_rows if int(r.get("is_default", 0)) == 1), "learner")
    return alias_map, canonical_set, default_role

def _get_role_maps(ttl_seconds: int = 60) -> Tuple[dict, set, str]:
    now = time.time()
    key = "roles"
    cached = _ROLE_CACHE.get(key)
    if cached and (now - cached[0] < ttl_seconds):
        return cached[1], cached[2], cached[3]
    alias_map, canonical_set, default_role = _load_role_maps()
    _ROLE_CACHE[key] = (now, alias_map, canonical_set, default_role)
    return alias_map, canonical_set, default_role

def normalize_role(value: Optional[str]) -> str:
    """
    Normalize a role using DB-configured aliases/canonicals.
    If unknown, returns the DB's default role.
    """
    v = (value or "").strip().lower()
    alias_map, canonical_set, default_role = _get_role_maps()
    if v in alias_map:
        v = alias_map[v]
    return v if v in canonical_set else default_role

def invalidate_role_cache() -> None:
    _ROLE_CACHE.pop("roles", None)

from sqlalchemy import text


def normalize_subject_slug(subject_in: str | None) -> str | None:
    s = (subject_in or "").strip().lower()
    if not s:
        return None
    return db.session.execute(text("""
        SELECT LOWER(COALESCE(slug, name))
        FROM auth_subject
        WHERE LOWER(COALESCE(slug, name)) = :s
        LIMIT 1
    """), {"s": s}).scalar()

def final_role(base_role: str | None, subject: str | None) -> str:
    """
    Global roles stay global ('admin','tenant','manager').
    Everything else is subject-scoped when a valid subject exists: '<subject>_<role>'.
    Unknown roles are normalized via DB (aliases + default).
    """
    role = normalize_role(base_role)
    if role in {"admin", "tenant", "manager"}:
        return role
    subj = normalize_subject_slug(subject)
    return f"{subj}_{role}" if subj else role


def is_admin(user=None):
    """
    True if current user is admin by any of:
      - user.is_admin (DB)
      - user.role == 'admin'
      - session['is_admin'] or session['role'] == 'admin'
    """
    if user is not None:
        if getattr(user, "is_admin", False):
            return True
        if (getattr(user, "role", "") or "").lower() == "admin":
            return True

    if current_user and getattr(current_user, "is_authenticated", False):
        if getattr(current_user, "is_admin", False):
            return True
        if (getattr(current_user, "role", "") or "").lower() == "admin":
            return True

    if session.get("is_admin"):
        return True
    if (session.get("role") or "").lower() == "admin":
        return True

    return False
