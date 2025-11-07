# app/admin_general/admin_tts.py
from flask import Blueprint, jsonify
from app.extensions import csrf  # wherever you init CSRFProtect(app)

tts_bp = Blueprint("admin_tts", __name__, url_prefix="/admin/general")

@tts_bp.route("/tts", methods=["POST"])
@csrf.exempt
def tts_api():
    # Server TTS retired. Client uses Edge/Web Speech.
    return jsonify({"error": "server-tts-retired", "use": "browser"}), 410
