# app/admin_general/routes.py
from flask import render_template, jsonify

from app.models.auth import AuthPricing, AuthSubject
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

@general_bp.route("/pricing")
def pricing_index():
    subject_slug = request.args.get("subject", "loss")

    # 1) Current subject
    subject = AuthSubject.query.filter_by(slug=subject_slug).first_or_404()

    # 2) All subjects for the dropdown
    all_subjects = AuthSubject.query.order_by(AuthSubject.name).all()

    # 3) Tiers only for this subject
    tiers = AuthPricing.query.filter_by(subject_id=subject.id) \
                             .order_by(AuthPricing.min_anchor_cents.asc()) \
                             .all()

    return render_template(
        "admin/pricing_index.html",
        subject=subject,
        all_subjects=all_subjects,
        tiers=tiers,
    )
