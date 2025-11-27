# app/admin_general/routes.py
import datetime
import os
import shutil
import subprocess
from flask import current_app, flash, redirect, render_template, jsonify, url_for
from flask_login import login_required
from sqlalchemy import text

from app.models.auth import AuthPricing, AuthSubject
from app.extensions import db
from . import general_bp
from flask import render_template, request, send_file, jsonify
import io, asyncio, time
import edge_tts

# keep your existing index()
@general_bp.get("/")
def index():
    return render_template("admin_general/index.html")
    #return render_template("admin_general/hub.html")

# (recommended) silence legacy POSTs to stop CSRF noise




# ---- UI ----
#@general_bp.get("/")
#def index():
    #return render_template("admin_general/index.html")

# ---- API ----
async def _tts_bytes(text, voice, rate, volume):
    comm = edge_tts.Communicate(text, voice, rate=rate, volume=volume)
    buf = io.BytesIO()
    async for chunk in comm.stream():
        if chunk["type"] == "audio":
            buf.write(chunk["data"])
    buf.seek(0)
    return buf

@general_bp.post("/tts")
def tts_api():
    data = request.get_json(force=True, silent=True) or {}
    text   = (data.get("text") or "").strip()
    voice  = data.get("voice") or "en-US-AriaNeural"
    rate   = data.get("rate") or "+0%"
    volume = data.get("volume") or "+0%"
    if not text:
        return jsonify({"error": "Text is empty"}), 400
    if len(text) > 5000:
        return jsonify({"error": "Text too long (limit 5000)"}), 413
    try:
        mp3 = asyncio.run(_tts_bytes(text, voice, rate, volume))
    except Exception as e:
        return jsonify({"error": f"TTS failed: {e}"}), 500
    fname = f"tts_{int(time.time())}.mp3"
    return send_file(mp3, mimetype="audio/mpeg", as_attachment=False, download_name=fname)

@general_bp.get("/tts-ui")
def tts_ui():
    return render_template("admin_general/tts.html")

@general_bp.route("/pricing/base")
def pricing_base():
    subject_slug = request.args.get("subject", "loss")

    subject = AuthSubject.query.filter_by(slug=subject_slug).first_or_404()
    all_subjects = AuthSubject.query.order_by(AuthSubject.name).all()

    rows = db.session.execute(
        text("""
            SELECT
                currency,
                amount_cents,
                plan,
                COALESCE(is_active, 1) AS is_active
            FROM auth_pricing
            WHERE subject_id = :sid
            ORDER BY
                COALESCE(active_from, created_at) DESC,
                COALESCE(updated_at, created_at) DESC,
                id DESC
        """),
        {"sid": subject.id},
    ).mappings().all()


    return render_template(
        "admin_general/pricing_base.html",
        subject=subject,
        all_subjects=all_subjects,
        prices=rows,
    )

@general_bp.route("/pricing", methods=["GET", "POST"])
def pricing_index():
    subject_slug = request.args.get("subject", "loss")

    subject = AuthSubject.query.filter_by(slug=subject_slug).first_or_404()
    all_subjects = AuthSubject.query.order_by(AuthSubject.name).all()

    # 1) Get ZAR anchor from auth_pricing (base price)
    anchor_row = db.session.execute(
        text("""
            SELECT amount_cents
              FROM auth_pricing
             WHERE subject_id = :sid
               AND plan = 'enrollment'
               AND COALESCE(is_active, 1) = 1
               AND (active_to IS NULL OR active_to > CURRENT_TIMESTAMP)
             ORDER BY
               COALESCE(active_from, created_at) DESC,
               COALESCE(updated_at, created_at) DESC,
               id DESC
             LIMIT 1
        """),
        {"sid": subject.id},
    ).first()

    anchor_zar_cents = (
        int(anchor_row.amount_cents)
        if anchor_row and anchor_row.amount_cents is not None
        else None
    )

    # Countries + default FX from ref table
    countries = db.session.execute(
        text("""
            SELECT
                alpha2     AS country_code,
                currency   AS currency_code,
                name,
                fx_to_zar
            FROM ref_country_currency
            WHERE COALESCE(is_active, TRUE) = TRUE
            ORDER BY name ASC
        """)
    ).mappings().all()

    # 2) Handle Add-country POST (add a new tier row)
    if request.method == "POST":
        country_code = (request.form.get("country_code") or "").strip().upper()
        fx_rate_raw  = (request.form.get("fx_rate") or "").strip()
        local_raw    = (request.form.get("local_amount_cents") or "").strip()

        if not country_code or not anchor_zar_cents:
            flash("Country code and a ZAR anchor are required.", "error")
        else:
            # 🔹 If FX not typed, try default from ref_country_currency
            if not fx_rate_raw:
                for c in countries:
                    if c.country_code == country_code:
                        if c.fx_to_zar is not None:
                            fx_rate_raw = str(c.fx_to_zar)
                        break

            local_amount_cents = None

            # Prefer explicit local_amount_cents if provided
            if local_raw:
                try:
                    local_amount_cents = int(local_raw)
                except ValueError:
                    flash("Local amount must be an integer number of cents.", "error")

            elif fx_rate_raw:
                try:
                    fx_rate = float(fx_rate_raw)
                    # ✅ fx_rate means "1 ZAR = fx_rate local currency"
                    local_amount_cents = int(round(anchor_zar_cents * fx_rate))
                except ValueError:
                    flash("FX rate must be a number.", "error")
            else:
                flash("Either enter a local amount or an FX rate.", "error")

            # Only insert if we ended up with a valid local_amount_cents
            # --- get base in ZAR cents (required) ---
            base_raw = (request.form.get("base_zar_cents") or "").strip()

            if not base_raw:
                flash("Please enter a base ZAR amount (cents).", "error")
                return redirect(url_for("general_bp.pricing_index", subject=subject.slug))

            try:
                base_zar_cents = int(base_raw)
            except ValueError:
                flash("Base ZAR amount must be whole cents (integer).", "error")
                return redirect(url_for("general_bp.pricing_index", subject=subject.slug))

            # --- existing insert, unchanged except zar uses base_zar_cents ---
            if local_amount_cents is not None:
                db.session.execute(
                    text("""
                        INSERT INTO subject_country_price
                            (subject_id, country_code, local_amount_cents, zar_amount_cents, is_active)
                        VALUES
                            (:sid, :cc, :local, :zar, 1)
                        ON CONFLICT (subject_id, country_code)
                        DO UPDATE SET
                            local_amount_cents = EXCLUDED.local_amount_cents,
                            zar_amount_cents   = EXCLUDED.zar_amount_cents,
                            is_active          = EXCLUDED.is_active
                    """),
                    {
                        "sid": subject.id,
                        "cc": country_code,
                        "local": local_amount_cents,
                        "zar": base_zar_cents,
                    },
                )
                db.session.commit()


                return redirect(url_for("general_bp.pricing_index", subject=subject.slug))

    # 3) Fetch existing tiers for display
    rows = db.session.execute(
        text("""
            SELECT
                country_code,
                local_amount_cents,
                zar_amount_cents,
                COALESCE(is_active, 1) AS is_active
            FROM subject_country_price
            WHERE subject_id = :sid
            ORDER BY country_code ASC
        """),
        {"sid": subject.id},
    ).mappings().all()

    return render_template(
        "admin_general/pricing_index.html",
        subject=subject,
        all_subjects=all_subjects,
        tiers=rows,
        anchor_zar_cents=anchor_zar_cents,
        countries=countries,
    )

@general_bp.route("/db-tools", methods=["GET"])
@login_required
def db_tools():
    return render_template("admin_general/db_tools.html")


@general_bp.get("/db-backup-now")
@login_required
def db_backup_now():
    """
    Create a PostgreSQL dump using pg_dump and stream it to the browser.
    - pg_dump: C:\Program Files\PostgreSQL\18\bin\pg_dump.exe
    - backup dir: D:\backups
    """

    engine = db.engine
    backend = engine.url.get_backend_name()
    if backend != "postgresql":
        flash(f"Backup is wired for PostgreSQL, but current engine is '{backend}'.", "error")
        return redirect(url_for("general_bp.db_tools"))

    # DSN for pg_dump (e.g. postgresql://user:pass@host/dbname)
    dsn = str(engine.url).replace("+psycopg2", "")

    pg_dump_path = r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"
    backup_dir   = r"D:\backups"

    if not os.path.exists(pg_dump_path):
        flash(f'pg_dump not found at "{pg_dump_path}".', "error")
        return redirect(url_for("general_bp.db_tools"))

    os.makedirs(backup_dir, exist_ok=True)

    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    fname = f"render_ait-{ts}.dump"
    backup_path = os.path.join(backup_dir, fname)

    try:
        subprocess.run(
            [pg_dump_path, "--format=custom", "--file", backup_path, dsn],
            check=True,
        )
    except Exception as e:
        current_app.logger.exception("pg_dump backup failed")
        flash(f"Backup failed: {e}", "error")
        return redirect(url_for("general_bp.db_tools"))

    flash("Backup created and saved in D:/backups", "success")

    return send_file(backup_path, as_attachment=True, download_name=fname)

@general_bp.route("/admin/general/app-backup-now")
@login_required
def app_backup_now():
    from datetime import datetime
    import zipfile, os, io

    # Where to save permanent snapshots
    BACKUP_DIR = r"D:/backups"
    os.makedirs(BACKUP_DIR, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    file_name = f"ait_app_{stamp}.zip"
    backup_path = os.path.join(BACKUP_DIR, file_name)

    # Folders to include (clean, minimal)
    APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    EXCLUDE = {"venv", "__pycache__", ".git", ".idea", "backups", "media"}

    # Create ZIP on disk
    with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(APP_ROOT):
            dirs[:] = [d for d in dirs if d not in EXCLUDE]
            for f in files:
                if f.endswith((".pyc", ".log")):
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, APP_ROOT)
                z.write(full, rel)
                
    flash("Backup created and saved in D:/backups", "success")

    # Serve ZIP for download
    return send_file(
        backup_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=file_name,
    )
