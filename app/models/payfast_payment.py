# app/config.py
import os

class Config:
    # --- PayFast base config ---------------------------------
    # sandbox toggle → used to derive PAYFAST_MODE if not set
    PAYFAST_SANDBOX = os.environ.get("PAYFAST_SANDBOX", "true").lower() == "true"

    # If PAYFAST_MODE isn’t set in env, derive it from PAYFAST_SANDBOX
    PAYFAST_MODE = os.environ.get(
        "PAYFAST_MODE",
        "sandbox" if PAYFAST_SANDBOX else "live",
    )

    PAYFAST_MERCHANT_ID  = os.environ.get("PAYFAST_MERCHANT_ID", "")
    PAYFAST_MERCHANT_KEY = os.environ.get("PAYFAST_MERCHANT_KEY", "")
    PAYFAST_PASSPHRASE   = os.environ.get("PAYFAST_PASSPHRASE", "")

    PAYFAST_RETURN_URL = os.environ.get(
        "PAYFAST_RETURN_URL",
        "https://mathwithhands.com/payments/success",
    )
    PAYFAST_CANCEL_URL = os.environ.get(
        "PAYFAST_CANCEL_URL",
        "https://mathwithhands.com/payments/checkout/cancel",
    )
    PAYFAST_NOTIFY_URL = os.environ.get(
        "PAYFAST_NOTIFY_URL",
        "https://mathwithhands.com/payments/notify",
    )
