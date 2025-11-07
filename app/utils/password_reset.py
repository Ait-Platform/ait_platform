# app/utils/password_reset.py
from itsdangerous import URLSafeTimedSerializer
from flask import current_app

def _s():
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt="pw-reset")

def make_reset_token(user_id: int) -> str:
    return _s().dumps({"uid": user_id})

def load_reset_token(token: str, max_age_seconds: int = 3600) -> int | None:
    try:
        data = _s().loads(token, max_age=max_age_seconds)
        return int(data.get("uid"))
    except Exception:
        return None
