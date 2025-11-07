# app/utils/branding.py
import os, base64
from flask import current_app

def get_logo_data_uri() -> str | None:
    """
    Returns data:image/png;base64,... for static/branding/ait_logo.png.
    Works in WeasyPrint/xhtml2pdf without remote fetch.
    """
    try:
        path = os.path.join(current_app.static_folder, "branding", "ait_logo.png")
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        return f"data:image/png;base64,{b64}"
    except Exception as e:
        # Will render without a logo if missing; logged for visibility
        current_app.logger.warning("PDF logo not found/loaded: %s", e)
        return None
