from flask import render_template, redirect, url_for, request, session
from . import sms_subject_bp

# Public marketing page for SMS
@sms_subject_bp.get("/about")
def sms_about():
    # keep a clean subject marker so the existing register flow sees it
    session.setdefault("reg_ctx", {})
    session["reg_ctx"]["subject"] = "sms"
    session.modified = True

    return render_template("subject/sms/about.html")


# Entry point to registration, reusing your existing auth register
@sms_subject_bp.get("/start")
def sms_start():
    next_url = request.args.get("next") or url_for("sms_admin_bp.sms_dashboard")
    return redirect(
        url_for(
            "auth_bp.register",
            role="school_admin",
            subject="sms",
            next=next_url,
        )
    )
