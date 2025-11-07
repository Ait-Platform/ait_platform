# Single source of truth for building the LOSS report context.
# We just re-export the service-layer builder to avoid circular imports.

from app.services.loss_report import build_report_ctx as build_context
