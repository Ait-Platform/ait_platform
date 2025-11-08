# app/config.py
import os

class Config:
    PAYFAST_MODE         = os.environ.get("PAYFAST_MODE", "sandbox")
    PAYFAST_MERCHANT_ID  = os.environ.get("PAYFAST_MERCHANT_ID", "")
    PAYFAST_MERCHANT_KEY = os.environ.get("PAYFAST_MERCHANT_KEY", "")
    PAYFAST_PASSPHRASE   = os.environ.get("PAYFAST_PASSPHRASE", "")
    PAYFAST_RETURN_URL   = os.environ.get("PAYFAST_RETURN_URL", "")
    PAYFAST_CANCEL_URL   = os.environ.get("PAYFAST_CANCEL_URL", "")
    PAYFAST_NOTIFY_URL   = os.environ.get("PAYFAST_NOTIFY_URL", "")
