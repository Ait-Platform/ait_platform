"""Property-only operations; no ownership schema or legacy wizard dependency."""
from flask import abort, flash, redirect, request, url_for, current_app, render_template, Response
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
    from app.program_billing.owner_management import owners_enabled, owner_rows
    enabled=owners_enabled()
    owners_by_property={p['id']:owner_rows(p['id']) for p in properties} if enabled else {}
    return render_pm_template("program_billing/property_owner.html", properties=properties,
        owners_enabled=enabled, owners_by_property=owners_by_property,
        error=error, property_name=name, ledger_workflow=request.args.get('workflow')=='ledger'), status


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


@billing_bp.route("/billing/property-owner/<int:property_id>/edit", methods=["GET", "POST"], endpoint="property_edit")
@login_required
def property_edit(property_id):
    statement = select(BilProperty.id, BilProperty.name, BilProperty.address, BilProperty.is_archived).where(
        BilProperty.id == property_id, BilProperty.manager_id == current_user.id)
    try:
        property_record = db.session.execute(statement.execution_options(autoflush=False)).mappings().first()
    except SQLAlchemyError:
        abort(503, description="Property could not be loaded.")
    if property_record is None:
        abort(404)
    values = dict(property_record)
    error = None
    status = 200
    if request.method == "POST":
        name = request.form.get("property_name", "").strip()
        values.update(name=name, is_archived=request.form.get("active") != "on")
        if not name or len(name) > 100:
            error = "Enter a property name of 1 to 100 characters."
            status = 400
        else:
            try:
                db.session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:scope), hashtext(lower(:name)))"),
                    {"scope": "pm_property_create:" + str(current_user.id), "name": name})
                duplicate = db.session.execute(text("SELECT id FROM bil_property WHERE manager_id=:manager AND id<>:property AND lower(name)=lower(:name) LIMIT 1"),
                    {"manager": current_user.id, "property": property_id, "name": name}).scalar_one_or_none()
                if duplicate is not None:
                    db.session.rollback()
                    error = "Another property in your register already has this name."
                    status = 409
                else:
                    updated = db.session.execute(text("UPDATE bil_property SET name=:name,is_archived=:archived WHERE id=:property AND manager_id=:manager RETURNING id"),
                        {"name": name, "archived": values["is_archived"], "property": property_id, "manager": current_user.id}).scalar_one_or_none()
                    if updated is None:
                        db.session.rollback()
                        abort(404)
                    db.session.commit()
                    flash("Property changes saved.", "success")
                    return redirect(url_for("billing_bp.property_edit", property_id=property_id))
            except SQLAlchemyError as exc:
                db.session.rollback()
                current_app.logger.error("PM property edit failed error=%s", type(exc).__name__)
                error = "Property changes could not be saved. Please try again or contact support."
                status = 503
    return render_pm_template("program_billing/property_edit.html", property=values, error=error), status


def portfolio_context():
    # Scalar property columns only: never load municipal, tenant or meter relations.
    properties=db.session.execute(text('SELECT * FROM bil_property WHERE manager_id=:u AND is_archived=false ORDER BY name,id'),
        {'u':current_user.id}).mappings().all()
    from app.program_billing.owner_management import owners_enabled, owner_rows
    enabled=owners_enabled()
    owners={p['id']:owner_rows(p['id'], include_inactive=True, include_history=True) for p in properties} if enabled else {}
    return dict(properties=properties,owners_by_property=owners,owners_enabled=enabled)


def portfolio_page():
    return render_pm_template("program_billing/property_portfolio.html", **portfolio_context())


@billing_bp.post("/billing/portfolio/email", endpoint="portfolio_email")
@login_required
def portfolio_email():
    from email_validator import validate_email, EmailNotValidError
    if request.form.get('intent')!='send':
        abort(400,description="Use Send Email to submit the report.")
    entered=request.form.get('recipient_email','').strip()
    context=portfolio_context()
    def failed(message,status=503):
        return render_pm_template("program_billing/property_portfolio.html",
            **context,email_open=True,recipient_email=entered,email_error=message),status
    try:
        recipient=validate_email(entered,check_deliverability=False).normalized
    except EmailNotValidError:
        return failed("Enter a valid recipient email address.",400)
    if current_app.config.get('MAIL_SUPPRESS_SEND'):
        return failed("Email sending is unavailable. No report was sent.")
    from app.utils.mailer import send_standard_report_email
    from app.program_billing.portfolio_pdf import build_portfolio_pdf
    try:
        pdf=build_portfolio_pdf(context)
        if not pdf or not pdf.startswith(b'%PDF-'):raise ValueError("Invalid report PDF")
        accepted=send_standard_report_email(to_email=recipient,subject="Property Portfolio",
            pdf_url=url_for('billing_bp.property_portfolio',_external=True),
            pdf_bytes=pdf,filename="Property_Portfolio.pdf",user_name=getattr(current_user,'name',None))
        if accepted is not True:raise RuntimeError("Sender did not confirm submission")
    except Exception as error:
        current_app.logger.error("Portfolio email failed error=%s",type(error).__name__)
        return failed("The report could not be submitted for email. Please retry or contact support.")
    flash(f"Property Portfolio submitted successfully for email to {recipient}.","success")
    return redirect(url_for('billing_bp.property_portfolio'))


@billing_bp.get("/billing/property-owner/<int:property_id>/owners/edit", endpoint="property_owners_edit")
@login_required
def property_owners_edit(property_id):
    property_record=db.session.execute(select(BilProperty.id,BilProperty.name).where(
        BilProperty.id==property_id,BilProperty.manager_id==current_user.id)).mappings().first()
    if property_record is None:abort(404)
    from app.program_billing.owner_management import owners_enabled,owner_rows
    enabled=owners_enabled()
    return render_pm_template("program_billing/property_owners_edit.html", property=property_record,
        owners_enabled=enabled,assigned_owners=owner_rows(property_id,include_inactive=True) if enabled else [])


@billing_bp.get("/billing/portfolio/print", endpoint="portfolio_print")
@login_required
def portfolio_print():
    response=Response(render_template("program_billing/portfolio_document.html",
        print_mode=True,**portfolio_context()),mimetype="text/html")
    response.headers['Cache-Control']='private, no-store'
    return response
