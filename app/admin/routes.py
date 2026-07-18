from flask import (
    Blueprint, current_app, render_template, 
    redirect, url_for, abort, request, session,
    jsonify, flash)
from app.models.loss import LcaResult
from app.utils import reading_utils
from app.utils.roles import is_admin  # reuse your helper
from app.extensions import db
from app.models.reading import RdpLesson
from . import admin_bp
#admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")
from sqlalchemy import select, func, text
from flask_login import current_user
import uuid
from app.models.payment import VoucherToken
from app.models.auth import AuthSubject

# subjects you support in admin
ALLOWED_SUBJECTS = {"reading", "home", "loss", "billing", "adv_math", "spv"}  # extend as needed

@admin_bp.before_request
def _guard():
    # Allow authenticated property managers to access billing statement routes
    if request.path.startswith('/admin/billing/') and current_user.is_authenticated:
        return None
        
    if not is_admin():
        return redirect(url_for("public_bp.welcome"))



@admin_bp.route("/<subject>/", endpoint="subject_dashboard")
def subject_dashboard(subject: str):
    subject = (subject or "").lower().strip()
    if subject not in ALLOWED_SUBJECTS:
        abort(404)
    return render_template(f"admin/{subject}/dashboard.html", subject=subject)

# --- Lessons list (simple) ---
@admin_bp.route("/<subject>/lessons", endpoint="lessons")
def lessons(subject: str):
    if subject != "reading":
        abort(404)
    lessons = db.session.query(RdpLesson).order_by(RdpLesson.order.asc(), RdpLesson.id.asc()).all()
    return render_template("admin/reading/lessons.html", lessons=lessons, subject=subject)

# --- Reorder UI ---
@admin_bp.route("/<subject>/reorder", methods=["GET"], endpoint="reorder")
def reorder(subject: str):
    if subject != "reading":
        abort(404)
    lessons = (
        db.session.query(RdpLesson.id, RdpLesson.title, RdpLesson.order)
        .order_by(RdpLesson.order.asc(), RdpLesson.id.asc())
        .all()
    )
    return render_template("admin/reading/reorder.html", lessons=lessons, subject=subject)

# --- Save new order (JSON) ---
@admin_bp.route("/api/<subject>/lessons/reorder", methods=["POST"], endpoint="api_reorder_lessons")
def api_reorder_lessons(subject: str):
    if subject != "reading":
        abort(404)
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids") or []
    if not ids or not all(isinstance(i, int) for i in ids):
        return jsonify(ok=False, error="Invalid ids"), 400

    # Only reorder the provided ids; we expect all lessons are present in the UI.
    for idx, lid in enumerate(ids, start=1):
        db.session.query(RdpLesson).filter_by(id=lid).update({"order": idx})
    db.session.commit()
    return jsonify(ok=True)

# ... existing admin_bp and routes ...

@admin_bp.route("/<subject>/lessons/new", methods=["GET", "POST"], endpoint="new_lesson")
def new_lesson(subject: str):
    subject = (subject or "").lower().strip()
    if subject != "reading":
        abort(404)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        caption = (request.form.get("caption") or "").strip()
        video_filename = (request.form.get("video_filename") or "").strip()

        if not title:
            flash("Title is required.", "warning")
            return render_template(
                "admin/reading/new_lesson.html",
                subject=subject, title=title,
                caption=caption, video_filename=video_filename
            )

        # Ensure NOT NULL at DB level
        video_filename = video_filename or ""

        # Validate extension (optional but helpful)
        allowed_ext = {"mp4", "webm", "ogg", "png", "jpg", "jpeg", "gif", "webp", "svg", "pdf"}
        if video_filename:
            ext = video_filename.rsplit(".", 1)[-1].lower() if "." in video_filename else ""
            if ext not in allowed_ext:
                flash(
                    f"Unsupported file type '.{ext}'. Allowed: {', '.join(sorted(allowed_ext))}.",
                    "warning"
                )
                return render_template(
                    "admin/reading/new_lesson.html",
                    subject=subject, title=title,
                    caption=caption, video_filename=video_filename
                )

        # Warn (non-blocking) if file not present under /static/
        # If no folder given, we check /static/videos/<file> (template will also default there).
        try:
            import os
            root = current_app.static_folder  # absolute path to /static
            candidate = video_filename
            if candidate and ("/" not in candidate and "\\" not in candidate):
                candidate = os.path.join("videos", candidate)  # default folder only for the check

            if candidate:
                # Normalize and ensure the path stays within /static (safety)
                fullpath = os.path.normpath(os.path.join(root, candidate))
                inside_static = os.path.commonpath([root, fullpath]) == os.path.normpath(root)
                if inside_static and not os.path.exists(fullpath):
                    flash(
                        f"Warning: '{candidate}' not found under /static/. "
                        "The lesson will save, but media won't render until the file is placed there.",
                        "warning"
                    )
        except Exception:
            # Non-fatal; just skip the existence hint
            pass

        # Append to end of sequence
        next_order = db.session.query(db.func.coalesce(db.func.max(RdpLesson.order), 0)).scalar() + 1

        lesson = RdpLesson(title=title, caption=caption, order=next_order)

        # Reading uses in-house media only
        if hasattr(lesson, "video_filename"):
            # Store exactly what the admin typed (template knows how to resolve it)
            lesson.video_filename = video_filename   # may be "" (NOT NULL safe)

        db.session.add(lesson)
        db.session.commit()
        flash("Lesson created.", "success")
        return redirect(url_for("admin_bp.lessons", subject="reading"))

    # GET
    return render_template("admin/reading/new_lesson.html", subject=subject)

# app/admin/reading/routes.py
@admin_bp.route("/reading/preview", methods=["GET"], endpoint="reading_preview")
def admin_reading_preview():
    # Admin gate
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    # Optional: preview as a specific learner ?as=email
    email = (request.args.get("as") or session.get("email") or "").strip().lower()

    # Single source of truth for learner dashboard data
    ctx = reading_utils.dashboard_context(email)

    # In preview we want all cards clickable (no gating)
    for item in ctx.get("items", []):
        item["can_start"] = True

    # Let the template show a "Back to Admin" link/banner if you want
    ctx["admin_preview"] = True

    return render_template("school_reading/learner_dashboard.html", **ctx)



# app/admin/routes.py (or wherever your admin_bp routes are)
# app/admin/routes.py
from flask import render_template, current_app
from . import admin_bp

# ---- Tiles dashboard view ----

# app/admin/routes.py
from flask import request, session, redirect, url_for

def _admins_only():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))  # not login
    return None

# /admin/ → templates/admin/admin_dashboard.html






from . import admin_bp
from flask import render_template, session, redirect, url_for

# --- define the view function ONCE ---
def _admin_dashboard_view():
    # Renders your main admin dashboard UI
    return render_template("admin/index.html", subjects=sorted(ALLOWED_SUBJECTS))

# --- bind BOTH endpoints to the same function (AFTER it's defined) ---
admin_bp.add_url_rule("/", endpoint="index",            view_func=_admin_dashboard_view)
admin_bp.add_url_rule("/", endpoint="admin_dashboard",  view_func=_admin_dashboard_view)

# app/blueprints/admin/loss_admin.py


@admin_bp.route("/loss/runs", methods=["GET"], endpoint="loss_runs_selector")
def loss_runs_selector():
    rows = db.session.execute(
        select(
            LcaResult.run_id.label("run_id"),
            func.max(LcaResult.created_at).label("last_at"),
            func.count().label("answers")
        )
        .where(LcaResult.run_id.isnot(None))
        .group_by(LcaResult.run_id)
        .order_by(func.max(LcaResult.created_at).desc())
    ).all()
    return render_template("admin/loss/runs_selector.html", runs=rows)

@admin_bp.route("/programs", methods=["GET", "POST"])
def manage_programs():
    from app.models.auth import AuthSubject
    if request.method == "POST":
        subject_id = request.form.get("subject_id")
        is_hidden = request.form.get("is_hidden") == "1"
        req_price = request.form.get("requires_price") == "1"
        ptype = request.form.get("program_type")
        subj = AuthSubject.query.get(subject_id)
        if subj:
            subj.is_hidden_on_bridge = is_hidden
            subj.requires_price = req_price
            subj.program_type = ptype
            db.session.commit()
            flash(f"Updated {subj.name}", "success")
        return redirect(url_for("admin_bp.manage_programs"))
    
    subjects = AuthSubject.query.order_by(AuthSubject.name).all()
    return render_template("admin/programs.html", subjects=subjects)


@admin_bp.route("/settings", methods=["GET", "POST"])
def global_settings():
    if request.method == "POST":
        quote_cents = request.form.get("mechanic_quote_cents")
        invoice_cents = request.form.get("mechanic_invoice_cents")
        enquiry_cents = request.form.get("practice_enquiry_cents")
        
        hds_cents = request.form.get("hds_subscription_cents")
        adv_reg_cents = request.form.get("adv_math_registration_cents")
        adv_sub_cents = request.form.get("adv_math_subtopic_cents")
        
        bil_base = request.form.get("bil_base_price")
        bil_inc = request.form.get("bil_included_meters")
        bil_extra = request.form.get("bil_extra_meter_price")
        
        updates = []
        if quote_cents: updates.append(('mechanic_quote_cents', quote_cents))
        if invoice_cents: updates.append(('mechanic_invoice_cents', invoice_cents))
        if enquiry_cents: updates.append(('practice_enquiry_cents', enquiry_cents))
        if hds_cents: updates.append(('hds_subscription_cents', hds_cents))
        if adv_reg_cents: updates.append(('adv_math_registration_cents', adv_reg_cents))
        if adv_sub_cents: updates.append(('adv_math_subtopic_cents', adv_sub_cents))
        

        for k, v in request.form.items():
            if k.startswith('visibility_') or k.startswith('yoco_mode_'):
                updates.append((k, v))
        for key, val in updates:
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES (:k, :v) ON CONFLICT(key) DO UPDATE SET value=excluded.value"), {"k": key, "v": val})
            
        from app.models.billing import BilPlatformSettings
        bil_settings = BilPlatformSettings.query.first()
        if not bil_settings:
            bil_settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
            db.session.add(bil_settings)
        if bil_base: bil_settings.base_price_cents = int(float(bil_base) * 100)
        if bil_inc: bil_settings.included_meters = int(bil_inc)
        if bil_extra: bil_settings.extra_meter_price_cents = int(float(bil_extra) * 100)
            
        db.session.commit()
        flash("Global settings updated successfully", "success")
        return redirect(url_for("admin_bp.global_settings"))
        
    settings = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    settings_dict = {s.key: s.value for s in settings}
    
    from app.models.billing import BilPlatformSettings
    bil_settings = BilPlatformSettings.query.first()
    if not bil_settings:
        bil_settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
        db.session.add(bil_settings)
        db.session.commit()
        
    return render_template("admin/settings.html", settings=settings_dict, bil_settings=bil_settings)

from app.models.auth import DirectMessage
@admin_bp.route('/messages', methods=['GET', 'POST'])
def view_messages():
    if request.method == 'POST':
        msg_id = request.form.get('msg_id')
        reply = request.form.get('reply')
        msg = DirectMessage.query.get(msg_id)
        if msg and reply:
            msg.reply = reply
            msg.is_read = True
            db.session.commit()
            flash('Reply sent successfully', 'success')
        return redirect(url_for('admin_bp.view_messages'))
        
    msgs = DirectMessage.query.order_by(DirectMessage.created_at.desc()).all()
    return render_template('admin/messages.html', messages=msgs)


@admin_bp.route("/modules_control", methods=["GET", "POST"], endpoint="modules_control")
def modules_control():
    if request.method == "POST":
        updates = []
        for k, v in request.form.items():
            if k.startswith('visibility_') or k.startswith('yoco_mode_'):
                updates.append((k, v))
        for key, val in updates:
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES (:k, :v) ON CONFLICT(key) DO UPDATE SET value=excluded.value"), {"k": key, "v": val})
        db.session.commit()
        flash("Module controls updated successfully", "success")
        return redirect(url_for("admin_bp.modules_control"))
        
    settings = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    settings_dict = {s.key: s.value for s in settings}
    return render_template("admin/modules_control.html", settings=settings_dict)

from app.models.payment import VoucherToken
from app.models.auth import AuthSubject
import uuid

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
