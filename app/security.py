# app/security.py
from typing import Mapping
import hmac, hashlib, os

_DEV_SECRET = os.getenv("DEV_PAY_SECRET", "changeme")

def make_dev_signature(order_id: str) -> str:
    return hmac.new(_DEV_SECRET.encode(), order_id.encode(), hashlib.sha256).hexdigest()

def verify_provider_signature(*, provider: str, payload: dict, expected_signature: str | None) -> None:
    if (provider or "").lower() != "dev":
        # add real providers later
        return
    if not expected_signature:
        raise ValueError("Missing signature")
    order_id = payload.get("order_id") or payload.get("merchant_reference") or ""
    good = make_dev_signature(order_id)
    if not hmac.compare_digest(good, expected_signature):
        raise ValueError("Bad dev signature")

# app/security.py
import hmac, hashlib, os



