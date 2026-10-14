"""Property-only operations; no ownership schema or legacy wizard dependency."""
from flask import abort, flash, redirect, request, url_for, current_app
from flask_login import current_user, login_required
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app.models.billing import BilProperty
from app.program_billing.routes import billing_bp
from app.program_billing.lease_management import render_pm_template


def property_owner_page():
    error = None
    status = 200
    name = ""
    if request.method == "POST":
        name = request.form.get("property_name", "").strip()
        if not name or len(name) > 100:
            error = "Enter a property name of 1 to 100 characters."
            status = 400
        else:
            try:
                # Serialize same-manager/name submissions, including concurrent retries.
                db.session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:scope), hashtext(lower(:name)))"),
                    {"scope": "pm_property_create:" + str(current_user.id), "name": name})
                existing = db.session.execute(text("SELECT id FROM bil_property WHERE manager_id=:manager AND lower(name)=lower(:name) ORDER BY id LIMIT 1"),
                    {"manager": current_user.id, "name": name}).scalar_one_or_none()
                if existing is None:
                    prop = BilProperty(name=name, manager_id=current_user.id, address="",
                        is_archived=False, onboarding_status="completed")
                    db.session.add(prop)
                    db.session.commit()
                    flash("Property saved. You can now assign its address.", "success")
                else:
                    db.session.rollback()
                    flash("That property is already in your register. Use its existing record below.", "info")
                return redirect(url_for("billing_bp.property_owner"))
            except SQLAlchemyError as exc:
                db.session.rollback()
                current_app.logger.error("PM property save failed error=%s", type(exc).__name__)
                error = "Property could not be saved. Please try again or contact support."
                status = 503
    statement = select(BilProperty.id, BilProperty.name, BilProperty.address, BilProperty.is_archived).where(
        BilProperty.manager_id == current_user.id).order_by(BilProperty.name, BilProperty.id)
    try:
        properties = db.session.execute(statement.execution_options(autoflush=False)).mappings().all()
    except SQLAlchemyError:
        abort(503, description="Your properties could not be loaded.")
    return render_pm_template("program_billing/property_owner.html", properties=properties,
        error=error, property_name=name), status


@billing_bp.post("/billing/property-owner/<int:property_id>/address", endpoint="property_address")
@login_required
def property_address(property_id):
    address = request.form.get("address", "").strip()
    if len(address) > 200:
        abort(400, description="Property address must be no more than 200 characters.")
    try:
        # Authorization remains in the mutation itself, including reassignment races.
        updated = db.session.execute(text("UPDATE bil_property SET address=:address WHERE id=:property AND manager_id=:manager RETURNING id"),
            {"address": address, "property": property_id, "manager": current_user.id}).scalar_one_or_none()
        if updated is None:
            db.session.rollback()
            abort(404)
        db.session.commit()
    except SQLAlchemyError as exc:
        db.session.rollback()
        current_app.logger.error("PM property address save failed error=%s", type(exc).__name__)
        abort(503, description="Address could not be saved.")
    flash("Property address saved.", "success")
    return redirect(url_for("billing_bp.property_owner"))
