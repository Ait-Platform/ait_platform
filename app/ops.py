from flask import Blueprint

ops_bp = Blueprint("ops_bp", __name__)

@ops_bp.route("/healthz")
def health_check():
    return "OK"

