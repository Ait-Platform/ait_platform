"""Lease Page 1: read-only property choice, retained only in the session."""
from flask import abort, redirect, request, session, url_for
from flask_login import current_user
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app.models.billing import BilProperty
from app.program_billing.routes import billing_bp
from app.program_billing.lease_management import require_pm_user, render_pm_template


@billing_bp.route("/billing/leases/new/property", methods=["GET", "POST"], endpoint="lease_property")
def lease_property():
    require_pm_user()
    statement = select(BilProperty.id, BilProperty.name, BilProperty.address).where(
        BilProperty.manager_id == current_user.id, BilProperty.is_archived == False
    ).order_by(BilProperty.name, BilProperty.id)
    try:
        properties = db.session.execute(statement.execution_options(autoflush=False)).mappings().all()
    except SQLAlchemyError:
        abort(503, description="Properties could not be loaded.")
    saved = session.get("lease_page1_property") or {}
    if not isinstance(saved, dict) or saved.get("user_id") != current_user.id:
        saved = {}
    selected = next((p for p in properties if p["id"] == saved.get("property_id")), None)
    if selected is None:
        session.pop("lease_page1_property", None)
    if request.method == "POST":
        requested = request.form.get("property_id", "")
        if not requested.isdecimal():
            abort(400, description="Select an existing property.")
        selected = next((p for p in properties if p["id"] == int(requested)), None)
        if selected is None:
            abort(404)
        session["lease_page1_property"] = {"user_id": current_user.id, "property_id": selected["id"]}
        return redirect(url_for("billing_bp.lease_property"))
    return render_pm_template("program_billing/lease_property.html", properties=properties,
        selected_property=selected)


@billing_bp.after_request
def protect_lease_property(response):
    if request.endpoint == "billing_bp.lease_property":
        response.headers["Cache-Control"] = "private, no-store"
    return response
