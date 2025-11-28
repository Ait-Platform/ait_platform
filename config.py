# config.py
import os
import re
from pathlib import Path
from flask import current_app

from app.scripts.blender import CLI



# ---- base directories -------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = os.environ.get("FLASK_INSTANCE_PATH", str(BASE_DIR / "instance"))
os.makedirs(INSTANCE_DIR, exist_ok=True)

# Default admin / bridge login email (used by app/__init__.py)
DEFAULT_LOGIN_EMAIL = os.getenv("DEFAULT_LOGIN_EMAIL", "ait@mathwithhands.com")

# where your canonical repo CSVs live
SEED_REPO_DIR = os.getenv("SEED_REPO_DIR")              # e.g. "<project>/seeds"
SEED_UPLOAD_ARCHIVE = os.getenv("SEED_UPLOAD_ARCHIVE")  # e.g. "<instance>/seed_uploads"

PG_DUMP_PATH = r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"
DB_BACKUP_DIR = r"D:\backups"

# Example of how to merge with your existing variables:

# -----------------------------
# CONFIG (with CLI overrides)
# -----------------------------
BASE_DIR = r"C:\Users\Sanjith\OneDrive\Documentos\LoloAd2025"

# Defaults
AD_TITLE_DEFAULT = "Adaptation Vector"
MAIN_TEXT_DEFAULT = "Have you lost a loved one? Measure your adaptation vector."
SUB_TEXT_DEFAULT = "Archoney Institute of Technology"

THEME_DEFAULT = "navy"

FPS = 30
SEC = 30
F_END = FPS * SEC

BLEND_OUT = os.path.join(BASE_DIR, "AIT_Adaptation_Vector.blend")
MP4_OUT = os.path.join(BASE_DIR, "ad.mp4")

# Optional audio defaults (can stay empty)
MUSIC_PATH = ""
VOICE_PATH = ""

# Apply CLI overrides if present
AD_TITLE = CLI.get("title", AD_TITLE_DEFAULT)
MAIN_TEXT = CLI.get("main_text", MAIN_TEXT_DEFAULT)
SUB_TEXT = CLI.get("sub_text", SUB_TEXT_DEFAULT)

THEME_KEY = CLI.get("theme", THEME_DEFAULT)

if "fps" in CLI:
    FPS = CLI["fps"]

if "frames" in CLI:
    F_END = CLI["frames"]
else:
    F_END = FPS * SEC

if "mp4_out" in CLI:
    MP4_OUT = CLI["mp4_out"]

if "music" in CLI:
    MUSIC_PATH = CLI["music"]

if "voice" in CLI:
    VOICE_PATH = CLI["voice"]



# ...



# ---- tiny helpers -----------------------------------------------------------
def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


_SENDER_RE = re.compile(r'^\s*(?P<name>.*?)\s*<\s*(?P<addr>[^>]+)\s*>\s*$')


def _parse_sender(val: str | None, fallback_name: str, fallback_addr: str):
    """
    Accepts either:
      - "Display Name <addr@example.com>"
      - "addr@example.com"
      - None -> falls back to (fallback_name, fallback_addr)
    Returns:
      - (name, addr) tuple, or
      - plain email string
    """
    if not val:
        return (fallback_name, fallback_addr)
    m = _SENDER_RE.match(val)
    if m:
        name = m.group("name").strip() or fallback_name
        addr = m.group("addr").strip() or fallback_addr
        return (name, addr)
    if "@" in val and "<" not in val and ">" not in val:
        return val.strip()
    return (fallback_name, fallback_addr)


# ---- seeds dir helper -------------------------------------------------------
def seeds_dir(subject: str = "loss") -> Path:
    base = current_app.config.get("SEEDS_DIR")
    base_path = Path(base) if base else Path(current_app.instance_path) / "seeds"
    d = base_path / subject
    d.mkdir(parents=True, exist_ok=True)
    return d


# ----------------------------------------------------------------------------
class Config:
    """
    Base configuration loaded by the app factory via:
      app.config.from_object("config.Config")
    """

    # ------------ Core / Security ------------
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")
    MAINTENANCE_MODE = int(os.getenv("MAINTENANCE_MODE", "0"))

    # ------------ Database (Postgres only) ------------
    _raw_db_url = os.getenv("DATABASE_URL", "").strip()

    # Render gives postgres://; normalize to postgresql+psycopg2://
    if _raw_db_url.startswith("postgres://"):
        _raw_db_url = _raw_db_url.replace(
            "postgres://",
            "postgresql+psycopg2://",
            1,
        )

    if not _raw_db_url:
        raise RuntimeError(
            "DATABASE_URL is not set – PostgreSQL is required (no SQLite fallback)."
        )

    SQLALCHEMY_DATABASE_URI = _raw_db_url
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}

    # ------------ Seeds ------------
    SEEDS_DIR = os.getenv("SEEDS_DIR")  # if set, overrides default

    # ------------ LOSS seed controls ------------
    LOSS_CSV = os.getenv("LOSS_CSV")
    LOSS_IMPORT_ON_BOOT = _to_bool(os.getenv("LOSS_IMPORT_ON_BOOT"), default=False)

    # ------------ Contact form / Mail (Zoho) ------------
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.zoho.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "info@mathwithhands.com")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

    MAIL_SERVER = os.getenv("MAIL_SERVER", SMTP_HOST)
    MAIL_PORT = int(os.getenv("MAIL_PORT", str(SMTP_PORT)))
    MAIL_USE_TLS = _to_bool(os.getenv("MAIL_USE_TLS", "1"))
    MAIL_USE_SSL = _to_bool(os.getenv("MAIL_USE_SSL", "0"))
    MAIL_USERNAME = os.getenv("MAIL_USERNAME", SMTP_USERNAME)
    MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", SMTP_PASSWORD)
    MAIL_DEFAULT_SENDER = _parse_sender(
        os.getenv("MAIL_DEFAULT_SENDER"),
        "AIT Platform",
        MAIL_USERNAME,
    )

    MAIL_SUPPRESS_SEND = _to_bool(os.getenv("MAIL_SUPPRESS_SEND", "0"), default=False)
    CONTACT_TO_EMAIL = os.getenv("CONTACT_TO_EMAIL", MAIL_USERNAME)

    # ------------ Misc / Debug / Cookies ------------
    DEBUG_TOOLBAR = _to_bool(os.getenv("DEBUG_TOOLBAR", "false"), default=False)

    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = False
    REMEMBER_COOKIE_SAMESITE = "Lax"
    REMEMBER_COOKIE_SECURE = False

    # dev default; Render can override to https
    PREFERRED_URL_SCHEME = os.getenv("PREFERRED_URL_SCHEME", "http")

    # ------------ App URL ------------
    APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:5000")

    # ------------ wkhtmltopdf ------------
    WKHTMLTOPDF_EXE = os.getenv(
        "WKHTMLTOPDF_EXE",
        r"C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe",
    )

    # ------------ PayFast ------------
    PAYMENT_GATEWAY = os.getenv("PAYMENT_GATEWAY", "payfast")

    PAYFAST_MODE = os.getenv("PAYFAST_MODE", "sandbox")
    PAYFAST_SANDBOX = _to_bool(os.getenv("PAYFAST_SANDBOX", "true"))

    PAYFAST_MERCHANT_ID = os.getenv("PAYFAST_MERCHANT_ID", "10000100")
    PAYFAST_MERCHANT_KEY = os.getenv("PAYFAST_MERCHANT_KEY", "46f0cd694581a")
    PAYFAST_PASSPHRASE = os.getenv("PAYFAST_PASSPHRASE", "")

    PAYFAST_RETURN_URL = os.getenv(
        "PAYFAST_RETURN_URL",
        "http://127.0.0.1:5000/payments/success",
    )
    PAYFAST_CANCEL_URL = os.getenv(
        "PAYFAST_CANCEL_URL",
        "http://127.0.0.1:5000/payments/cancel",
    )
    PAYFAST_NOTIFY_URL = os.getenv(
        "PAYFAST_NOTIFY_URL",
        "http://127.0.0.1:5000/payments/notify",
    )
