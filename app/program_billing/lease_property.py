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


@billing_bp.route("/billing/leases/new/parties", methods=["GET", "POST"], endpoint="lease_parties")
def lease_parties():
    require_pm_user()
    selection = session.get("lease_page1_property") or {}
    if not isinstance(selection, dict) or selection.get("user_id") != current_user.id:
        return redirect(url_for("billing_bp.lease_property"))
    statement = select(BilProperty.id, BilProperty.name, BilProperty.address).where(
        BilProperty.id == selection.get("property_id"),
        BilProperty.manager_id == current_user.id, BilProperty.is_archived == False
    )
    try:
        selected = db.session.execute(statement.execution_options(autoflush=False)).mappings().first()
    except SQLAlchemyError:
        abort(503, description="Property access could not be verified.")
    if selected is None:
        session.pop("lease_page1_property", None)
        session.pop("lease_page2_parties", None)
        return redirect(url_for("billing_bp.lease_property"))
    saved = session.get("lease_page2_parties") or {}
    if not isinstance(saved, dict) or saved.get("user_id") != current_user.id or saved.get("property_id") != selected["id"]:
        saved = {}
    parties = saved.get("parties", {})
    error = None
    if request.method == "POST":
        parties = {}
        for role in ("lessor", "lessee"):
            fields = {}
            for key, limit in (("party_type", 16), ("name", 120), ("email", 160), ("phone", 40)):
                value = request.form.get(role + "_" + key, "").strip()
                if len(value) > limit:
                    abort(400, description="Party field exceeds its maximum length.")
                fields[key] = value
            if fields["party_type"] not in ("", "individual", "company", "trust"):
                abort(400, description="Invalid party type.")
            parties[role] = fields
        session["lease_page2_parties"] = {"user_id": current_user.id,
            "property_id": selected["id"], "parties": parties}
        if request.form.get("action") == "previous":
            return redirect(url_for("billing_bp.lease_property"))
        if any(not p["party_type"] or not p["name"] for p in parties.values()):
            error = "Enter a party type and name for both Lessor and Lessee."
        else:
            return redirect(url_for("billing_bp.lease_parties"))
    return render_pm_template("program_billing/lease_parties.html", selected_property=selected,
        parties=parties, error=error)


@billing_bp.after_request
def protect_lease_property(response):
    if request.endpoint in {"billing_bp.lease_property", "billing_bp.lease_parties"}:
        response.headers["Cache-Control"] = "private, no-store"
    return response
