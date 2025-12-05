# app/__init__.py
import os as _os
from flask_migrate import migrate
from app.payments.pricing import number_to_words, price_cents_for
from app.bootstrap.subjects import ensure_core_subjects
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
from datetime import datetime
from pathlib import Path
import logging, uuid
import matplotlib
matplotlib.use("Agg")
from flask import Flask, g, request, current_app
from flask_login import LoginManager, current_user
from sqlalchemy import text, event
from sqlalchemy import text as sa_text
from sqlalchemy import event
from jinja2 import select_autoescape
from flask_mail import Message  # only Message here
from app.extensions import mail, db, login_manager, csrf
from app.models.loss import LcaRun, LcaResult
from app.models.auth import User
from config import DEFAULT_LOGIN_EMAIL, Config
from os import getenv
import os  # sqlite3 no longer needed
import click
from hashlib import sha256
from app.models.visit import VisitLog
from app.models.payment import Payment, Subscription
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)  # picks up your .env locally
from flask_wtf.csrf import generate_csrf
from datetime import date
from flask import Flask, render_template_string

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

def create_app():
    app = Flask(__name__, instance_relative_config=True, 
                #template_folder=str(TEMPLATES_DIR),
        template_folder="../templates",
        static_folder="../static",)
    
    Path(app.instance_path).mkdir(parents=True, exist_ok=True)

    # 1) Base config object (config.py at project root)
    app.config.from_object("config.Config")

    # 2) Instance overrides (instance/config.py) – safe if missing
    app.config.from_pyfile("config.py", silent=True)

    # 3) Environment overrides (e.g., FLASK_SQLALCHEMY_DATABASE_URI)
    app.config.from_prefixed_env()

               
    # 3) Init extensions AFTER config
    db.init_app(app)
    csrf.init_app(app)
    mail.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth_bp.login"

    # ⬇ add this near the end of create_app, before `return app`
    with app.app_context():
        db.create_all()

    # 4) Template helpers
    app.jinja_env.globals.update(csrf_token=generate_csrf)
    app.jinja_env.autoescape = select_autoescape(['html', 'htm', 'xml'])
    app.jinja_env.globals['number_to_words'] = number_to_words
    
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    app.config.from_mapping(
        SMTP_HOST=os.getenv("SMTP_HOST", "smtp.zoho.com"),
        SMTP_PORT=int(os.getenv("SMTP_PORT", 587)),
        SMTP_USERNAME=os.getenv("SMTP_USERNAME"),
        SMTP_PASSWORD=os.getenv("SMTP_PASSWORD"),
        CONTACT_TO_EMAIL=os.getenv("CONTACT_TO_EMAIL", os.getenv("SMTP_USERNAME")),
    )

    app.config["LOSS_FREE"] = os.getenv("LOSS_FREE", "false").lower() in ("1", "true", "yes")

    # (optional) ensure keys are present – maps OS envs directly if Config missed any

    for k in (
        "PAYFAST_MODE","PAYFAST_MERCHANT_ID","PAYFAST_MERCHANT_KEY",
        "PAYFAST_PASSPHRASE","PAYFAST_RETURN_URL","PAYFAST_CANCEL_URL",
        "PAYFAST_NOTIFY_URL"
    ):
        if k not in app.config or not app.config[k]:
            v = os.environ.get(k)
            if v: app.config[k] = v

    @app.context_processor
    def inject_now():
        return {"now": datetime.utcnow}
    
    # make {{ csrf_token() }} available
    @app.context_processor
    def inject_csrf():
        return {"csrf_token": generate_csrf}
    
    @app.before_request
    def _inject_country():
        g.country_iso2 = (request.headers.get('cf-ipcountry') or 'ZA').upper()



    @app.before_request
    def log_site_hit():
        # skip obvious noise / health / static
        if request.endpoint in ("static",) or request.path.startswith("/healthz"):
            return

        try:
            uid   = current_user.id if getattr(current_user, "is_authenticated", False) else None
            authd = bool(getattr(current_user, "is_authenticated", False))
        except Exception:
            uid, authd = None, False

        ua = (request.headers.get("User-Agent") or "")[:255]

        try:
            db.session.execute(
                sa_text("""
                    INSERT INTO site_hit (path, user_id, is_auth, user_agent)
                    VALUES (:path, :uid, :authd, :ua)
                """),
                {"path": request.path, "uid": uid, "authd": authd, "ua": ua},
            )
            db.session.commit()
        except Exception:
            # don’t break the site if logging fails
            db.session.rollback()

    @app.context_processor
    def _inject_helpers():
        return dict(price_cents_for=price_cents_for)

    @login_manager.user_loader
    def load_user(user_id: str):
        from app.models.auth import User
        try:
            return db.session.get(User, int(user_id))
        except Exception:
            return None

    
    register_cli(app)  # ← registers the two CLI commands
 

    @app.cli.command("mail-test")
    def mail_test():
        to = current_app.config.get("CONTACT_TO_EMAIL") or current_app.config["MAIL_USERNAME"]
        msg = Message(
            subject="AIT mail test",
            recipients=[to],
            # IMPORTANT: don't set sender here; use MAIL_DEFAULT_SENDER
            body="If you see this, SMTP is working. – AIT Platform"
        )
        mail.send(msg)
        print(f"Sent test email to {to} via {current_app.config.get('MAIL_SERVER')}:{current_app.config.get('MAIL_PORT')}")

    # Register a simple CLI to create/seed ref_country_currency (SQLite-safe)
    @app.cli.command("seed-currencies")
    def seed_currencies():
        sql_create = """
        CREATE TABLE IF NOT EXISTS ref_country_currency (
        alpha2     TEXT PRIMARY KEY,
        currency   TEXT NOT NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
        sql_seed = """
        INSERT INTO ref_country_currency (alpha2, currency) VALUES
        ('ZA','ZAR'),('US','USD'),('GB','GBP'),('IN','INR'),('IE','EUR')
        ON CONFLICT(alpha2) DO UPDATE SET currency = excluded.currency;
        """
        from sqlalchemy import text
        db.session.execute(text(sql_create))
        db.session.execute(text(sql_seed))
        db.session.commit()
        print("ref_country_currency: created/seeded")

    @app.route("/healthz")
    def healthz():
        return "ok", 200
    
    @app.cli.command("seed-all-currencies")
    def seed_all_currencies():
        # Lazy import so app can start even if Babel isn't installed
        try:
            from babel.numbers import get_territory_currency
        except Exception as e:
            print("Babel is required for this command. Install with: pip install Babel")
            raise

        # Ensure table
        db.session.execute(text("""
        CREATE TABLE IF NOT EXISTS ref_country_currency (
            alpha2     TEXT PRIMARY KEY,
            currency   TEXT NOT NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """))

        # Gather ISO alpha-2 from your utils list
        try:
            from app.utils.country_list import _name_code_iter, COUNTRIES
        except ImportError:
            from utils.country_list import _name_code_iter, COUNTRIES

        from datetime import date
        today = date.today()
        seen = set()
        codes = []
        for nm, cd in _name_code_iter(COUNTRIES):
            cd = (cd or "").strip().upper()
            if len(cd) == 2 and cd.isalpha() and cd not in seen:
                seen.add(cd); codes.append(cd)

        overrides = {"AQ": "USD", "XK": "EUR"}  # edge cases
        inserted = 0; missing = []

        for code in codes:
            ccy = overrides.get(code)
            if not ccy:
                try:
                    ccy = get_territory_currency(code, date=today)
                except Exception:
                    ccy = None

            if not ccy:
                missing.append(code); continue

            db.session.execute(
                text("""
                INSERT INTO ref_country_currency (alpha2, currency)
                VALUES (:a, :c)
                ON CONFLICT(alpha2)
                DO UPDATE SET currency = excluded.currency,
                                updated_at = CURRENT_TIMESTAMP
                """),
                {"a": code, "c": ccy}
            )
            inserted += 1

        db.session.commit()
        print(f"ref_country_currency: upserted {inserted} rows.")
        if missing:
            print("No currency found for:", ", ".join(sorted(missing)))

    @app.cli.command("fix-fx-schema")
    def fix_fx_schema():
        from sqlalchemy import text
        db.session.execute(text("""
            ALTER TABLE ref_country_currency
            ADD COLUMN IF NOT EXISTS fx_to_zar NUMERIC
        """))
        db.session.commit()
        print("OK: fx_to_zar column ensured on ref_country_currency")

    @app.before_request
    def _trace_in():
        g.reqid = str(uuid.uuid4())[:8]
        try:
            user_id = getattr(request, "user_id", None)
        except Exception:
            user_id = None
        app.logger.info(
            "[%s] → %s %s ep=%s args=%s form=%s user_id=%s",
            g.reqid, request.method, request.path, request.endpoint,
            dict(request.args), dict(request.form), user_id
        )

    @app.after_request
    def _trace_out(resp):
        rid = getattr(g, "reqid", "????")
        loc = resp.headers.get("Location", "")
        if loc:
            app.logger.info("[%s] ← %s redirect to %s", rid, resp.status, loc)
        else:
            app.logger.info("[%s] ← %s", rid, resp.status)
        return resp

    @app.before_request
    def _log_visit():
        # skip noise and admin static
        if request.endpoint in ("static",):
            return
        try:
            ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
            ip_hash = sha256(ip.encode("utf-8")).hexdigest()[:16] if ip else ""
            ua = (request.user_agent.string or "")[:250]
            uid = getattr(current_user, "id", None)
            path = (request.path or "")[:250]
            db.session.add(VisitLog(path=path, user_id=uid, ip_hash=ip_hash, ua=ua))
            db.session.commit()
        except Exception:
            db.session.rollback()
       
    def _flag_welcome_redirect(resp):
        # Keep this merged with the other after_request if you prefer; I’m separating for clarity.
        loc = resp.headers.get("Location")
        if resp.status_code in (301,302,303,307,308) and loc and "/welcome" in loc:
            app.logger.warning("[%s] ⚠ redirect to /welcome detected (auth/role guard?)", getattr(g, "reqid", "????"))
        return resp

    if os.getenv("MAINTENANCE_MODE") == "1":
        @app.before_request
        def show_maintenance():
            return render_template_string("""
            <html style="text-align:center;padding-top:20vh;font-family:sans-serif">
            <h1>We'll be right back</h1>
            <p>Archoney Institute of Technology is undergoing scheduled maintenance.</p>
            </html>
            """), 503

    # 5) Blueprints
    from app.public.routes import public_bp
    from app.auth.routes import auth_bp
    from app.subject_reading.routes import reading_bp
    from app.school_home.routes import home_bp
    from app.subject_loss.routes import loss_bp
    from app.school_billing.routes import billing_bp
    from app.admin import admin_bp
    from app.admin_general.routes import general_bp      # GET /admin/general/
    from app.admin_general.admin_tts import tts_bp       # POST /admin/general/tts
    from app.payments.payfast import payfast_bp
    from app.admin.sms import sms_admin_bp
    from app.subject_sms import sms_subject_bp

    #app.logger.warning("registered checkout_bp at /checkout")

    #app.register_blueprint(checkout_bp)
    app.register_blueprint(public_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(reading_bp)
    app.register_blueprint(home_bp)
    app.register_blueprint(loss_bp)
    app.register_blueprint(billing_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(sms_subject_bp)
    app.register_blueprint(sms_admin_bp)
    app.register_blueprint(general_bp, url_prefix="/admin/general")
    app.register_blueprint(tts_bp, url_prefix="/admin/general")
    app.register_blueprint(payfast_bp, url_prefix="/payments")
    
    #csrf.exempt(checkout_bp)  # keeps webhook/start happy
    # Exempt ONLY the PayFast IPN route (or the whole blueprint if you prefer)
    csrf.exempt(payfast_bp)  # or: add @csrf.exempt on the /notify function

    # Log admin routes only in debug
    if app.debug:
        with app.app_context():
            for r in app.url_map.iter_rules():
                if r.endpoint.startswith("admin_bp."):
                    print("ADMIN ROUTE:", r.endpoint, "->", r.rule)

    # app/filters.py
    def _to_number_0_100(value):
        # Accepts 75, "75", "75%", None -> returns 0..100
        try:
            s = str(value).strip()
            if s.endswith("%"):
                s = s[:-1]
            n = float(s)
        except Exception:
            n = 0.0
        if n < 0:
            n = 0.0
        if n > 100:
            n = 100.0
        return int(n)

    def pct_width(value) -> str:
        """Return a valid CSS width with %, always safe."""
        return f"{_to_number_0_100(value)}%"

    def pct_num(value) -> int:
        """Return an int 0..100 (for aria attrs, labels, etc.)."""
        return _to_number_0_100(value)

    # 7) Create tables + helper view
    with app.app_context():
        db.create_all()
        # Create Postgres-safe view (no missing columns)
        # ensure core subjects exist in auth_subject (SQLite + Postgres)
        ensure_core_subjects()

        # Create Postgres-safe view (no missing columns)
        engine_name = db.engine.name  # 'sqlite', 'postgresql', etc.

        if engine_name == "postgresql":
            sql = """
                CREATE OR REPLACE VIEW approved_admins AS
                SELECT
                    email,
                    ''::text   AS subject,
                    1::integer AS active
                FROM auth_approved_admin;
            """
        elif engine_name == "sqlite":
            sql = """
                CREATE VIEW IF NOT EXISTS approved_admins AS
                SELECT
                    email,
                    '' AS subject,
                    1  AS active
                FROM auth_approved_admin;
            """
        else:
            sql = """
                CREATE VIEW approved_admins AS
                SELECT
                    email,
                    '' AS subject,
                    1  AS active
                FROM auth_approved_admin;
            """

        db.session.execute(sa_text(sql))
        db.session.commit()

    @app.route("/__routes")
    def __routes():
        return "<br>".join(sorted(f"{r.endpoint} → {r.rule}" for r in app.url_map.iter_rules()))

    @app.route("/__blueprints")
    def __blueprints():
        return "<br>".join(sorted(f"{name} → {bp.import_name}" for name, bp in app.blueprints.items()))


        # Optional: only run if explicitly enabled
        #if os.getenv("LOSS_IMPORT_ON_BOOT", "0") == "1":
            #scoring_import.maybe_import_on_boot()
    #app.logger.warning("Jinja autoescape reset to select_autoescape(['html','htm','xml']).")
    #app.logger.warning("registered payfast_bp at /payments")    
    #app.logger.info(f"[DB] Using {db.engine.url}")
        # --- temporary diagnostics (keep while debugging) ---
    try:
        for p in app.jinja_loader.searchpath:  # type: ignore[attr-defined]
            app.logger.info(f"[JINJA] searchpath: {p}")
    except Exception:
        pass
    # ----------------------------------------------------
        from app.admin.seed_cli import init_app as init_seed_cli
        init_seed_cli(app)
    # ---- seed/repair default admin (dev-safe) ----
           # seed default admin (optional)

        email = (DEFAULT_LOGIN_EMAIL or "").strip().lower()
        if email:
            u = User.query.filter_by(email=email).first()
            if not u:
                u = User(name="Admin", email=email, country="South Africa", role="admin")
                u.set_password(getenv("DEFAULT_LOGIN_PASSWORD", "123"))
                db.session.add(u)
                db.session.commit()
    return app

def register_cli(app):
    @app.cli.command("user-create")
    @click.argument("email")
    @click.argument("password")
    @click.option("--name", default="Admin")
    @click.option("--role", default="admin")
    def user_create(email, password, name, role):
        """Create a user with the given credentials."""
        from app.models.auth import User
        email = email.strip().lower()
        if User.query.filter_by(email=email).first():
            click.echo(f"User already exists: {email}")
            return
        u = User(name=name, email=email, role=role)
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        click.echo(f"Created user: {email}")


# --- ADD THIS BLOCK BELOW ---
    @app.cli.command("mail-test")
    @click.option("--to", "to_addr", default=None, help="Override recipient email. Defaults to MAIL_USERNAME.")
    def mail_test(to_addr):
        """Send a quick test email using current SMTP settings."""
        # mail is the shared instance from app.extensions, already init_app(app)'d in create_app
        with app.app_context():
            to = (to_addr or app.config.get("MAIL_USERNAME") or "").strip()
            if not to:
                click.echo("No recipient found. Use --to or set MAIL_USERNAME in .env.", err=True)
                return
            msg = Message(
                subject="AIT mail test",
                recipients=[to],
                body="If you received this, your SMTP settings are working.",
            )
            mail.send(msg)
            click.echo(f"Sent test email to {to}")
            
# Keep this helper separate; name avoids shadowing seed_cli.register_cli

def send_mail(to, subject, html):
    msg = Message(subject=subject, recipients=[to])
    msg.html = html
    mail.send(msg)

def send_result_email_via_mail(to_email, pdf_bytes, filename, subject, body):
    msg = Message(subject=subject, recipients=[to_email], body=body)
    msg.attach(filename, "application/pdf", pdf_bytes)
    mail.send(msg)

def _send_pdf_email_via_mail(to_email, pdf_bytes, filename, subject, body):
    msg = Message(subject=subject, recipients=[to_email])
    msg.body = body
    msg.attach(filename, "application/pdf", pdf_bytes)
    mail.send(msg)



