#app/admin/security/modules_control.py
import uuid
from flask import abort, flash, redirect, render_template, request, url_for
from app.models.auth import AuthSubject
from app.models.payment import VoucherToken
from app.extensions import db
from app.utils.roles import is_admin
from app.admin import admin_bp
from flask_login import current_user

@admin_bp.route("/modules_control", methods=["GET", "POST"], endpoint="modules_control")
def modules_control():
    from sqlalchemy import text
    from app.extensions import db
    from app.models.auth import AuthSubject

    if request.method == "POST":

        # Update every active subject
        subjects = (
            AuthSubject.query
            .filter_by(is_active=1)
            .all()
        )

        for subject in subjects:

            # Welcome page visibility
            subject.show_on_welcome = (
                #request.form.get(f"show_on_welcome_{subject.slug}") == "on"
                f"show_on_welcome_{subject.slug}" in request.form
            )
        
            # Yoco Mode (still stored in system_settings)
            yoco_mode = request.form.get(
                f"yoco_mode_{subject.slug}",
                "sandbox"
            )

            db.session.execute(
                text("""
                    INSERT INTO system_settings (key, value)
                    VALUES (:k, :v)
                    ON CONFLICT (key)
                    DO UPDATE SET value = EXCLUDED.value
                """),
                {
                    "k": f"yoco_mode_{subject.slug}",
                    "v": yoco_mode
                }
            )

        db.session.commit()

        flash("Platform Module Control updated successfully.", "success")
        return redirect(url_for("admin_bp.modules_control"))

    # Read settings
    # Read settings
    settings = db.session.execute(
        text("SELECT key, value FROM system_settings")
    ).fetchall()

    settings_dict = {row.key: row.value for row in settings}

    print("=" * 60)
    print("SYSTEM SETTINGS:", len(settings_dict))
    print(settings_dict)
    print("=" * 60)

    # --------------------------------------------------
    # RAW SQL TEST
    # --------------------------------------------------
    rows = db.session.execute(text("""
        SELECT *
        FROM auth_subject
        ORDER BY name
    """)).mappings().all()

    print("RAW ROWS:", len(rows))

    subjects = rows

    for r in rows:
        print(r)

    print("=" * 60)

    # --------------------------------------------------
    # ORM TEST
    # --------------------------------------------------

    subjects = (
        AuthSubject.query
        .order_by(AuthSubject.name)
        .all()
    )

    print("ORM SUBJECT COUNT:", len(subjects))

    for s in subjects:
        print(
            s.id,
            s.name,
            s.slug,
            s.show_on_welcome
        )

    print("=" * 60)

    return render_template(
        "admin/modules_control.html",
        settings=settings_dict,
        subjects=subjects
    )

@admin_bp.route("/vouchers", methods=["GET", "POST"], endpoint="manage_vouchers")
def manage_vouchers():
    if not is_admin():
        abort(403)
        
    if request.method == "POST":
        subject_id = request.form.get("subject_id", type=int)
        value_amount = request.form.get("value_amount", type=int)
        code = request.form.get("code")
        
        if not subject_id or not value_amount:
            flash("Subject and Value Amount are required.", "danger")
        else:
            if not code:
                # Generate a random 8-character uppercase code
                code = str(uuid.uuid4()).upper()[:8]
            
            # Check if code exists
            exists = VoucherToken.query.filter_by(code=code).first()
            if exists:
                flash("That voucher code already exists!", "danger")
            else:
                v = VoucherToken(
                    code=code, 
                    value_amount=value_amount, 
                    subject_id=subject_id,
                    created_by_user_id=current_user.id
                )
                db.session.add(v)
                db.session.commit()
                flash(f"Voucher {code} generated successfully!", "success")
        return redirect(url_for("admin_bp.manage_vouchers"))

    # GET request
    vouchers = VoucherToken.query.order_by(VoucherToken.created_at.desc()).all()
    subjects = AuthSubject.query.order_by(AuthSubject.name.asc()).all()
    
    return render_template("admin/vouchers.html", vouchers=vouchers, subjects=subjects)


