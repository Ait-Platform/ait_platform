# app/admin/loss/report_context.py
# Shim to avoid circular imports and admin URL leakage.
from app.school_loss.report_context import (
    build_context,
    render_report_html as _render_report_html,
)
__all__ = ["build_context", "_render_report_html"]
