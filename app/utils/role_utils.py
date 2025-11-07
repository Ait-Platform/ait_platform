# app/utils/role_utils.py
import os
from flask import url_for
from flask import session

def get_prefill_email():
    if os.environ.get("FLASK_ENV") == "development":
        return  "san@gmail.com"            #"loss@gmail.com"
    return ""

# app/utils/role_utils.py

def get_dashboard_route(role, subject=None, with_params=False):
    role = (role or "").strip().lower()
    subject = (subject or "").strip().lower()

    base_map = {
        "tenant": "billing_bp.tenant_dashboard",
        "manager": "billing_bp.manager_dashboard",
        "admin":  "admin_bp.index",  # generic admin landing
        "reading_learner": "reading_bp.learner_dashboard",
        "reading_tutor":   "reading_bp.tutor_dashboard",
        "home_learner":    "home_bp.learner_dashboard",
        "home_tutor":      "home_bp.tutor_dashboard",
        "loss_user":       "loss_bp.dashboard",
        "general_user":    "public_bp.welcome",
    }

    # Prefer subject-specific admin if role is admin and we know the subject
    if role == "admin":
        if subject == "reading":
            return ("admin_bp.reading_home", {}) if with_params else "admin_bp.reading_home"
        if subject == "billing":
            return ("admin_bp.billing_home", {}) if with_params else "admin_bp.billing_home"
        return ("admin_bp.index", {}) if with_params else "admin_bp.index"
    
    # Also allow explicit subject admin roles like "reading_admin"
    if role.endswith("_admin"):
        key = role.rsplit("_admin", 1)[0]
        endpoint = "admin_bp.subject_dashboard"
        params = {"subject": key}
        return (endpoint, params) if with_params else endpoint

    endpoint = base_map.get(role, "public_bp.welcome")
    return (endpoint, {}) if with_params else endpoint

#registration_helpers.py

def get_prefill_email():
    # First try session email, else return empty string
    return session.get("email", "san@gmail.com")

def get_registration_url(role: str, subject: str) -> str:
    """
    Constructs the URL to start registration with the given role and subject.
    """
    return url_for("auth_bp.start_registration", role=role, subject=subject)


def _norm(s: str | None) -> str | None:
    return s.strip() if (s and isinstance(s, str)) else None

def _norm_email(email: str | None) -> str | None:
    e = _norm(email)
    return e.lower() if e else None

def _valid_password(pw: str | None) -> bool:
    # minimal example; adjust to your policy
    return bool(pw and len(pw) >= 3)

# app/utils/role_utils.py
# still in app/utils/role_utils.py
from flask import session
try:
    from flask_login import current_user
except Exception:
    current_user = None


