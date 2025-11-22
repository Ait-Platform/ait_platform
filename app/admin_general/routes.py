# app/admin_general/routes.py
from flask import flash, redirect, render_template, jsonify, url_for
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

    # NEW: load country picker options from ref_country_currency
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
                    # anchor_zar_cents is in cents; fx_rate is local per 1 ZAR
                    local_amount_cents = int(round(anchor_zar_cents * fx_rate))
                except ValueError:
                    flash("FX rate must be a number.", "error")
            else:
                flash("Either enter a local amount or an FX rate.", "error")

            # Only insert if we ended up with a valid local_amount_cents
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
                        "zar": anchor_zar_cents,
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

