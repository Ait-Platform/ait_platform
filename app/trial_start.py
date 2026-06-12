# app/trial_start.py  (registered as endpoint "trial_start" in app factory)

from flask import redirect, request, url_for
from flask_login import current_user
from flask import session


'''
def start_trial():
    slug = (request.args.get("subject") or "").strip().lower() or "loss"

    # force plan into reg_ctx so the gatekeeper never “forgets” this is trial
    ctx = session.setdefault("reg_ctx", {})
    ctx["subject"] = slug
    ctx["plan"] = "trial"
    session.modified = True

    # Not logged in -> go REGISTER (not login), and come back here after decision
    if not getattr(current_user, "is_authenticated", False):
        return redirect(url_for(
            "auth_bp.show_register",
            role="user",
            subject=slug,
            next=url_for("trial_start", subject=slug),
        ))

    # Logged in -> grant trial entitlement, then enter via central gate
    ensure_trial_entitlement(user_id=int(current_user.id), product_slug=slug)
    return redirect(url_for("program_entry", subject_slug=slug))
'''
