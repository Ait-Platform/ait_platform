# config.py
import os
from pathlib import Path
import re
from flask import current_app

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
INSTANCE_DIR = os.path.join(BASE_DIR, "instance")
# ------------ App-level constants ------------
DEFAULT_LOGIN_EMAIL = os.getenv("DEFAULT_LOGIN_EMAIL", "san@gmail.com")

# Project base directory (this file's folder)
#BASE_DIR = Path(__file__).resolve().parent

# where your canonical repo CSVs live
SEED_REPO_DIR = os.getenv("SEED_REPO_DIR")  # e.g. set to "<project>/seeds"
# optional: where to archive admin uploads for audit
SEED_UPLOAD_ARCHIVE = os.getenv("SEED_UPLOAD_ARCHIVE")  # e.g. "<instance>/seed_uploads"


# config.py
class Config:
    # ...
    SEEDS_DIR = os.getenv("SEEDS_DIR")  # if set, overrides default

# routes.py
def seeds_dir(subject: str = "loss") -> Path:
    base = current_app.config.get("SEEDS_DIR")
    base_path = Path(base) if base else Path(current_app.instance_path) / "seeds"
    d = base_path / subject
    d.mkdir(parents=True, exist_ok=True)
    return d

def _to_bool(value: str, default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")

def _sqlite_uri_from(path: Path) -> str:
    # Use forward slashes for SQLAlchemy URIs on Windows
    return "sqlite:///" + path.as_posix()

# config.py


# ---- tiny helpers -----------------------------------------------------------
def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}

_SENDER_RE = re.compile(r'^\s*(?P<name>.*?)\s*<\s*(?P<addr>[^>]+)\s*>\s*$')

def _parse_sender(val: str | None, fallback_name: str, fallback_addr: str):
    """
    Accepts either:
      - "Display Name <addr@example.com>" (string)
      - "addr@example.com" (string)
      - None -> falls back to (fallback_name, fallback_addr)
    Returns a Flask-Mail compatible value:
      - tuple (name, addr) if we have both
      - plain email string if only an address was provided
    """
    if not val:
        return (fallback_name, fallback_addr)
    m = _SENDER_RE.match(val)
    if m:
        name = m.group("name").strip() or fallback_name
        addr = m.group("addr").strip() or fallback_addr
        return (name, addr)
    # if it's just an email without angle brackets, pass it through
    if "@" in val and "<" not in val and ">" not in val:
        return val.strip()
    # otherwise fall back
    return (fallback_name, fallback_addr)

# ---- base directories -------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
# allow overriding instance path via env; else use app/instance next to this file
INSTANCE_DIR = os.environ.get("FLASK_INSTANCE_PATH", str(BASE_DIR / "instance"))

# Ensure instance folder exists if used for sqlite/db files
os.makedirs(INSTANCE_DIR, exist_ok=True)

# ----------------------------------------------------------------------------
class Config:
    """
    Base configuration loaded by the app factory via:
      app.config.from_object("config.Config")
    """

    # ------------ Core / Security ------------
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")

    # ------------ Database ------------
    # Prefer DATABASE_URL from .env; otherwise stable sqlite in instance/
    SQLALCHEMY_DATABASE_URI = (
        os.getenv("DATABASE_URL")
        or f"sqlite:///{os.path.join(INSTANCE_DIR, 'data.db')}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # SQLite-friendly engine options (safe on other DBs too)
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
    }

    # ------------ LOSS seed controls ------------
    LOSS_CSV = os.getenv("LOSS_CSV")
    LOSS_IMPORT_ON_BOOT = _to_bool(os.getenv("LOSS_IMPORT_ON_BOOT"), default=False)

    # ------------ Mail ------------
    # If SENDGRID_API_KEY present → default to SendGrid else Gmail, but allow explicit overrides
    _USING_SENDGRID = bool(os.getenv("SENDGRID_API_KEY"))

    MAIL_SERVER = os.getenv("MAIL_SERVER") or (
        "smtp.sendgrid.net" if _USING_SENDGRID else "smtp.gmail.com"
    )
    MAIL_PORT = int(os.getenv("MAIL_PORT", "587"))
    MAIL_USE_TLS = _to_bool(os.getenv("MAIL_USE_TLS", "true"), default=True)
    MAIL_USE_SSL = _to_bool(os.getenv("MAIL_USE_SSL", "false"), default=False)

    # Username / Password depend on provider unless explicitly set
    MAIL_USERNAME = (
        os.getenv("MAIL_USERNAME")
        or ("apikey" if _USING_SENDGRID else os.getenv("GMAIL_USERNAME", ""))  # optional fallback
    )
    MAIL_PASSWORD = (
        os.getenv("MAIL_PASSWORD")
        or (os.getenv("SENDGRID_API_KEY") if _USING_SENDGRID else os.getenv("GMAIL_APP_PASSWORD", ""))  # optional fallback
    )

    # Prefer a single-string MAIL_DEFAULT_SENDER from .env if provided.
    # Otherwise use MAIL_FROM_NAME / MAIL_FROM_ADDR (your existing pattern).
    MAIL_DEFAULT_SENDER = _parse_sender(
        os.getenv("MAIL_DEFAULT_SENDER"),
        os.getenv("MAIL_FROM_NAME", "AIT Platform"),
        os.getenv("MAIL_FROM_ADDR", "no-reply@yourdomain.com"),
    )

    MAIL_SUPPRESS_SEND = os.getenv("MAIL_SUPPRESS_SEND","0").lower() in ("1","true","yes","on")

    # ------------ Stripe (optional) ------------
    STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
    STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "")
    STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

    # ------------ Misc ------------
    PREFERRED_URL_SCHEME = os.getenv("PREFERRED_URL_SCHEME", "https")
    DEBUG_TOOLBAR = _to_bool(os.getenv("DEBUG_TOOLBAR", "false"), default=False)

    # ------------ Contact form (Zoho SMTP) ------------
    # Used by public_bp.contact route to send mail directly via SMTP.
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.zoho.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "info@mathwithhands.com")  # your Zoho mailbox
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")  # app password if MFA is on
  

    # ------------ Mail (Zoho) ------------
    MAIL_SERVER = os.getenv("MAIL_SERVER", "smtp.zoho.com")
    MAIL_PORT = int(os.getenv("MAIL_PORT", "587"))
    MAIL_USE_TLS = _to_bool(os.getenv("MAIL_USE_TLS", "1"))
    MAIL_USE_SSL = _to_bool(os.getenv("MAIL_USE_SSL", "0"))
    MAIL_USERNAME = os.getenv("MAIL_USERNAME")
    MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
    MAIL_DEFAULT_SENDER = _parse_sender(
        os.getenv("MAIL_DEFAULT_SENDER"),
        "AIT Platform",
        os.getenv("MAIL_USERNAME", "info@mathwithhands.com"),
    )

    CONTACT_TO_EMAIL = os.getenv("CONTACT_TO_EMAIL", MAIL_USERNAME)

    STRIPE_PUBLIC_KEY = os.getenv("STRIPE_PUBLIC_KEY", "")
    STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    STRIPE_CURRENCY = os.getenv("STRIPE_CURRENCY", "zar")

    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE   = False
    REMEMBER_COOKIE_SAMESITE = "Lax"
    REMEMBER_COOKIE_SECURE   = False
    PREFERRED_URL_SCHEME     = "http"
    # IMPORTANT for dev: do NOT set SERVER_NAME or SESSION_COOKIE_DOMAIN
    # SECRET_KEY must be a fixed, non-random value (don’t rotate between requests)

