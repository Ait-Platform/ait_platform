# app/__init__.py
import os as _os
from app.payments.pricing import number_to_words
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
from flask_wtf import CSRFProtect
from flask_wtf.csrf import generate_csrf
from flask_login import LoginManager
from sqlalchemy import text, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from jinja2 import select_autoescape
from flask_mail import Message  # only Message here
from app.extensions import mail
from app.extensions import db, login_manager, mail  # <-- use the one from extensions
from app.models.loss import LcaRun, LcaResult
from app.models.auth import User
from config import DEFAULT_LOGIN_EMAIL
from os import getenv
import click
from hashlib import sha256
from flask_login import current_user
from app.models.visit import VisitLog
from app.models.payment import Payment, Subscription
import os, sqlite3
from werkzeug.middleware.proxy_fix import ProxyFix



csrf = CSRFProtect()

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

#BASE_DIR = Path(__file__).resolve().parent
#TEMPLATES_DIR = BASE_DIR / "templates"

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

    # 4) Last-resort fallback if nothing set
    # Stripe config
    app.config["STRIPE_SECRET_KEY"] = _os.getenv("STRIPE_SECRET_KEY")  # sk_test_...
    app.config["STRIPE_CURRENCY"]   = (_os.getenv("STRIPE_CURRENCY") or "zar").lower()

    # Database URI bootstrap (your existing logic, but with _os)
    if not app.config.get("SQLALCHEMY_DATABASE_URI"):
        data_dir = _os.getenv("AIT_DATA_DIR") or _os.path.join(_os.getcwd(), "instance_data")
        _os.makedirs(data_dir, exist_ok=True)
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + _os.path.join(data_dir, "data.db")

    uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")

    # --- Default to a local instance DB outside OneDrive if none provided ---
    import os, sqlite3
    from sqlalchemy import event
    from sqlalchemy.engine import Engine
    from sqlalchemy.pool import NullPool

    # Default DB location if none provided
    if not app.config.get("SQLALCHEMY_DATABASE_URI"):
        data_dir = os.getenv("AIT_DATA_DIR") or os.path.join(os.getcwd(), "instance_data")
        os.makedirs(data_dir, exist_ok=True)
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(data_dir, "data.db")

    uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")

    if uri.startswith("sqlite"):
        # one connection per request; avoids stale cross-thread handles
        opts = app.config.setdefault("SQLALCHEMY_ENGINE_OPTIONS", {})
        opts["poolclass"] = NullPool
        connect_args = opts.setdefault("connect_args", {})
        connect_args.setdefault("timeout", 5)                     # fail fast
        connect_args.setdefault("isolation_level", "IMMEDIATE")   # grab lock up-front
        # connect_args.setdefault("check_same_thread", False)  # only if needed

        # Install PRAGMAs only once; do NOT open a connection here
        if not app.config.get("_SQLITE_PRAGMAS_INSTALLED"):
            @event.listens_for(Engine, "connect")
            def _set_sqlite_pragmas(dbapi_connection, connection_record):
                if isinstance(dbapi_connection, sqlite3.Connection):
                    cur = dbapi_connection.cursor()
                    cur.execute("PRAGMA journal_mode=WAL;")
                    cur.execute("PRAGMA synchronous=NORMAL;")
                    cur.execute("PRAGMA busy_timeout=5000;")
                    cur.execute("PRAGMA foreign_keys=ON;")
                    cur.close()
            app.config["_SQLITE_PRAGMAS_INSTALLED"] = True

                
    # 3) Init extensions AFTER config
    db.init_app(app)
    csrf.init_app(app)
    mail.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth_bp.login"


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

    @app.context_processor
    def inject_now():
        return {"now": datetime.utcnow}

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

    @app.route("/healthz")
    def healthz():
        return "ok", 200

    # app/__init__.py  (after app = Flask(__name__) and db.init_app(app))
    

    @app.cli.command("create-stripe-tables")
    def create_stripe_tables():
        """Create fallback 'stripe_payment' / 'stripe_subscription' tables if missing."""
        from sqlalchemy import inspect
        insp = inspect(db.engine)
        to_create = []
        if "stripe_payment" not in insp.get_table_names():
            to_create.append(Payment.__table__)
        if "stripe_subscription" not in insp.get_table_names():
            to_create.append(Subscription.__table__)
        if to_create:
            with app.app_context():
                for t in to_create:
                    t.create(bind=db.engine, checkfirst=True)
            print("OK: created", [t.name for t in to_create])
        else:
            print("Nothing to create; tables already exist.")



    # Verbose logs
    logging.basicConfig(level=logging.DEBUG)
    app.logger.setLevel(logging.DEBUG)
    
    # --- debug: list routes in logs ---
    try:
        for r in app.url_map.iter_rules():
            app.logger.info("ROUTE %-28s %s", r.endpoint, r.rule)
    except Exception as e:
        app.logger.warning("Could not list routes: %s", e)
    # ----------------------------------

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

    # 5) Blueprints
    from app.public.routes import public_bp
    from app.auth.routes import auth_bp
    from app.subject_reading.routes import reading_bp
    from app.school_home.routes import home_bp
    from app.subject_loss.routes import loss_bp
    from app.school_billing.routes import billing_bp
    from app.admin import admin_bp
    from app.checkout import checkout_bp
    from app.admin.school_fee_management.routes import sfm_bp
    from app.admin_general.routes import general_bp      # GET /admin/general/
    from app.admin_general.admin_tts import tts_bp       # POST /admin/general/tts
    from app.payments.payfast import payfast_bp
    

    app.logger.warning("registered checkout_bp at /checkout")

    app.register_blueprint(checkout_bp)
    app.register_blueprint(public_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(reading_bp)
    app.register_blueprint(home_bp)
    app.register_blueprint(loss_bp)
    app.register_blueprint(billing_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(sfm_bp)
    app.register_blueprint(general_bp, url_prefix="/admin/general")
    app.register_blueprint(tts_bp,      url_prefix="/admin/general")
    app.register_blueprint(payfast_bp, url_prefix="/payments")
    
    csrf.exempt(checkout_bp)  # keeps webhook/start happy


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
        db.session.execute(text("""
            CREATE VIEW IF NOT EXISTS approved_admins AS
            SELECT email,
                   COALESCE(subject,'') AS subject,
                   COALESCE(active,1)   AS active
            FROM auth_approved_admin
        """))
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
    app.logger.warning("Jinja autoescape reset to select_autoescape(['html','htm','xml']).")
    app.logger.warning("registered payments_bp at /payments")    
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
    @click.option("--country", default="South Africa")
    @click.option("--role", default="admin")
    def user_create(email, password, name, country, role):
        """Create a user with the given credentials."""
        from app.models.auth import User
        email = email.strip().lower()
        if User.query.filter_by(email=email).first():
            click.echo(f"User already exists: {email}")
            return
        u = User(name=name, email=email, country=country, role=role)
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
            
    @app.cli.command("fix-overall-schema")
    def fix_overall_schema():
        """Ensure lca_overall_item has type/ordinal/active + indexes (SQLite-safe)."""
        from sqlalchemy import text
        from app.extensions import db

        def has_col(tab, col):
            rows = db.session.execute(text(f"PRAGMA table_info({tab})")).fetchall()
            return any(r[1] == col for r in rows)

        tab = "lca_overall_item"

        # Add missing columns (idempotent; prints error if already exists)
        if not has_col(tab, "type"):
            db.session.execute(text("ALTER TABLE lca_overall_item ADD COLUMN type TEXT NOT NULL DEFAULT 'summary'"))
        if not has_col(tab, "ordinal"):
            db.session.execute(text("ALTER TABLE lca_overall_item ADD COLUMN ordinal INTEGER NOT NULL DEFAULT 0"))
        if not has_col(tab, "active"):
            db.session.execute(text("ALTER TABLE lca_overall_item ADD COLUMN active INTEGER NOT NULL DEFAULT 1"))

        # Normalize values
        db.session.execute(text("""
            UPDATE lca_overall_item
            SET type = COALESCE(NULLIF(type,''), 'summary')
            WHERE type IS NULL OR type = ''
        """))

        # Helpful indexes
        for stmt in [
            "CREATE INDEX IF NOT EXISTS ix_lca_overall_item_type    ON lca_overall_item(type)",
            "CREATE INDEX IF NOT EXISTS ix_lca_overall_item_band    ON lca_overall_item(band)",
            "CREATE INDEX IF NOT EXISTS ix_lca_overall_item_active  ON lca_overall_item(active)",
            "CREATE INDEX IF NOT EXISTS ix_lca_overall_item_ordinal ON lca_overall_item(ordinal)",
        ]:
            db.session.execute(text(stmt))

        db.session.commit()

        cols = db.session.execute(text("PRAGMA table_info(lca_overall_item)")).fetchall()
        print("Columns now:", [c[1] for c in cols])
        print("OK")

        @app.cli.command("user-reset-password")
        @click.argument("email")
        @click.argument("password")
        def user_reset_password(email, password):
            """Reset a user's password."""
            from app.models.auth import User
            email = email.strip().lower()
            u = User.query.filter_by(email=email).first()
            if not u:
                click.echo(f"User not found: {email}")
                return
            u.set_password(password)
            db.session.commit()
            click.echo(f"Password reset for: {email}")

# Keep this helper separate; name avoids shadowing seed_cli.register_cli
def register_migration_cli(app):
    import click
    @app.cli.command("migrate-response-unique")
    def migrate_response_unique_cmd():
        """Rebuild lca_response so uniqueness is (run_id, question_id)."""
        try:
            # drop broken view if present
            db.session.execute(text("DROP VIEW IF EXISTS approved_admins;"))
            ddl = db.session.execute(text("""
                SELECT sql FROM sqlite_master
                WHERE type='table' AND name='lca_response'
            """)).scalar() or ""
            needs = ("unique" in ddl.lower()
                     and "user_id" in ddl.lower()
                     and "question_id" in ddl.lower())

            db.session.execute(text("PRAGMA foreign_keys=OFF;"))
            db.session.execute(text("PRAGMA legacy_alter_table=ON;"))

            if needs:
                click.echo("Migrating table (removing UNIQUE(user_id,question_id))...")
                db.session.execute(text("""
                    CREATE TABLE lca_response_new (
                      id          INTEGER PRIMARY KEY,
                      user_id     INTEGER NOT NULL,
                      run_id      INTEGER NOT NULL,
                      question_id INTEGER NOT NULL,
                      answer      VARCHAR(3) NOT NULL,
                      created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                      FOREIGN KEY(question_id) REFERENCES lca_question (id) ON DELETE CASCADE
                    )
                """))
                db.session.execute(text("""
                    INSERT INTO lca_response_new (id, user_id, run_id, question_id, answer, created_at)
                    SELECT id, user_id, run_id, question_id, answer, created_at
                    FROM lca_response
                """))
                db.session.execute(text("DROP TABLE lca_response;"))
                db.session.execute(text("ALTER TABLE lca_response_new RENAME TO lca_response;"))

            db.session.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_response_run_q
                ON lca_response(run_id, question_id)
            """))
            db.session.execute(text("CREATE INDEX IF NOT EXISTS ix_response_user ON lca_response(user_id)"))
            db.session.execute(text("CREATE INDEX IF NOT EXISTS ix_response_run  ON lca_response(run_id)"))

            db.session.execute(text("PRAGMA legacy_alter_table=OFF;"))
            db.session.execute(text("PRAGMA foreign_keys=ON;"))
            db.session.commit()
            click.echo("OK")
        except Exception as e:
            db.session.rollback()
            click.echo(f"ERROR: {e}", err=True)
            raise

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



