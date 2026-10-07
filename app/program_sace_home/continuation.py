"""Short-lived HOME authentication intent, separate from pending invitation data."""
import hashlib
import time
from urllib.parse import urljoin, urlparse
from flask import request, session

KEY = "sace_home_current_journey"
DESTINATIONS = {"join": "home_sace_bp.claim_assignment", "provisioning": "home_sace_bp.provision"}
PENDING = {"join": "sace_home_pending_code", "provisioning": "sace_home_provisioning_token"}


def clear():
    session.pop(KEY, None)


def begin(kind):
    value = session.get(PENDING[kind], "")
    session[KEY] = {"kind": kind, "pending_hash": hashlib.sha256(value.encode()).hexdigest(),
                    "expires_at": time.time() + 900}


def consume():
    destination = resolve()
    clear()
    return destination


def resolve():
    """Validate without consuming intent while shared routing compares activities."""
    context = session.get(KEY)
    if not isinstance(context, dict):
        clear()
        return None
    kind = context.get("kind")
    expiry = context.get("expires_at")
    if kind not in PENDING or not isinstance(expiry, (int, float)) or not time.time() < expiry:
        clear()
        return None
    value = session.get(PENDING[kind], "")
    if not isinstance(value, str) or not value or hashlib.sha256(value.encode()).hexdigest() != context.get("pending_hash"):
        clear()
        return None
    from werkzeug.exceptions import HTTPException
    from . import service as s
    try:
        row = s.invitation() if kind == 'join' else s.provisioning()
        s.consent('auditor' if kind == 'join' else 'controller', row.id)
    except HTTPException:
        clear()
        return None
    return DESTINATIONS[kind]


def safe_home_next(target):
    if not target:
        return False
    parsed = urlparse(urljoin(request.host_url, target))
    return (parsed.scheme in ("http", "https") and parsed.netloc == urlparse(request.host_url).netloc
            and parsed.path.startswith("/sace/home/"))


def discard_abandoned():
    """Navigation away invalidates HOME intent without reading/changing LITRE state."""
    if (KEY not in session and "sace_home_provisioning_context" not in session) or request.endpoint == "static":
        return
    if request.path in {"/sace/home/provisioning", "/sace/home/join", "/sace/home/pledge",
                        "/sace/home/authenticate", "/sace/home/claim"}:
        return
    if request.path in {"/login", "/register"}:
        target = request.values.get("next")
        subject = request.values.get("subject")
        if (not target or safe_home_next(target)) and (not subject or subject == "sace_home_endorsement"):
            return
    clear()
    if session.pop("sace_home_provisioning_context", None) is not None:
        session.pop("sace_home_provisioning_token", None)
        session.pop("sace_home_pledge_controller", None)
