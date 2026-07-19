from flask import render_template, request, session, redirect, url_for, flash
from flask_login import current_user
from sqlalchemy import text as sa_text
from app.enrollment.logic import get_quote_for_subject_country
from app.models.auth import AuthSubject, UserEnrollment
from app.models.payment import RefCountryCurrency, SubjectCountryPrice
from app.extensions import db
from datetime import datetime

from app.utils.redirects import safe_next
from . import quote_bp


def get_active_countries():
    return (
        RefCountryCurrency.query
        .filter_by(is_active=True)
        .order_by(RefCountryCurrency.name)
        .all()
    )

# 1. Quote route
@quote_bp.get("/quote")
def quote():
    subject_slug = (request.args.get("subject") or "").strip().lower()
    next_url = safe_next(request.args.get("next"), default="/")
    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code and current_user.is_authenticated and hasattr(current_user, 'country_code'):
        country_code = (current_user.country_code or "").strip().upper()

    subject = (
        AuthSubject.query
        .filter(db.func.lower(AuthSubject.slug) == subject_slug)
        .first()
    )
    if not subject or int(getattr(subject, "is_active", 0) or 0) != 1:
        flash("This program is currently unavailable.", "warning")
        return redirect(url_for("public_bp.welcome"))

    is_upgrade = False
    # Anti-exploitation: If logged in, check if they already have an enrollment for this subject
    if current_user.is_authenticated:
        row = db.session.execute(
            sa_text("""
                SELECT status 
                FROM user_enrollment 
                WHERE user_id = :uid AND subject_id = :sid 
                ORDER BY id DESC LIMIT 1
            """),
            {"uid": current_user.id, "sid": subject.id}
        ).first()
        if row and row.status in ("pending", "started"):
            flash("You already have an active quote. Resuming checkout...", "info")
            return redirect(url_for("yoco_bp.yoco_start", email=current_user.email, subject=subject.slug))
        elif row and row.status in ("active", "completed"):
            from app.utils.routing import get_dashboard_route
            
            # Check if it's an expired subscription (like BudgetCash)
            is_expired = False
            full_row = None
            if row.status == "active":
                now = datetime.utcnow()
                # We need to query the actual row to get trial_end, expires_at, and zar_amount_cents
                full_row = db.session.execute(
                    sa_text("""
                        SELECT trial_end, expires_at, zar_amount_cents
                        FROM user_enrollment 
                        WHERE user_id = :uid AND subject_id = :sid 
                        ORDER BY id DESC LIMIT 1
                    """),
                    {"uid": current_user.id, "sid": subject.id}
                ).first()
                if full_row:
                    if full_row.expires_at and full_row.expires_at < now:
                        is_expired = True
                    elif full_row.trial_end and full_row.trial_end < now and not full_row.expires_at:
                        is_expired = True
                    elif subject.slug == 'home' and not full_row.expires_at and not full_row.trial_end:
                        is_expired = True
                        
            if not is_expired:
                # Use actual assigned role name if it exists, otherwise pass None to let subject fallback handle it
                role_name = None
                if current_user.user_roles:
                    # Get the name of their first role
                    role_name = getattr(current_user.user_roles[0].role, 'name', None)
                    
                ep = get_dashboard_route(role_name, subject.slug)
                return redirect(url_for(ep))
            else:
                is_upgrade = True
                # Expired subscription: Force them to Yoco to lock in their existing old price
                # BUT don't do this for HOME upgrades, as they need to go through parity pricing quote page
                if subject.slug != 'home' and full_row and full_row.zar_amount_cents and int(full_row.zar_amount_cents) > 0:
                    flash("Redirecting to secure checkout at your locked renewal rate.", "info")
                    return redirect(url_for("yoco_bp.yoco_start", email=current_user.email, subject=subject.slug))

        # Anti-exploitation & convenience: Inherit country from previous enrollments
        prev_enr = db.session.execute(
            sa_text("""
                SELECT country_code 
                FROM user_enrollment 
                WHERE user_id = :uid AND country_code IS NOT NULL 
                ORDER BY id ASC LIMIT 1
            """),
            {"uid": current_user.id}
        ).first()

        if prev_enr and not country_code:
            country_code = prev_enr.country_code

    if not country_code:
        country_code = session.get("country_code", "")

    if country_code:
        session["country_code"] = country_code
        print("Quote: storing country_code =", country_code)

    commercial_mode = (getattr(subject, "commercial_mode", None) or "free").strip().lower()
    trial_days = int(getattr(subject, "trial_days", 0) or 0)
    requires_price = int(getattr(subject, "requires_price", 0) or 0)
    trial_allowed = bool(trial_days > 0)

    price_ctx = {
        "has_quote": False,
        "price_id": None,
        "country_code": None,
        "local_amount": None,
        "local_currency": None,
        "estimated_zar": None,
        "fx_rate": None,
        "is_discount": False,
    }

    row = None  # ensure row is always defined

    if country_code:
        row = get_quote_for_subject_country(subject.id, country_code)
        
        # Auto-generate parity price if missing
        if not row:
            za_price = SubjectCountryPrice.query.filter_by(subject_id=subject.id, country_code='ZA').first()
            if za_price:
                from app.payments.pricing import fx_rate_local_to_zar
                c_ref = RefCountryCurrency.query.filter_by(alpha2=country_code).first()
                if c_ref:
                    fx = fx_rate_local_to_zar(country_code)
                    local_cents = int(za_price.zar_amount_cents * fx) if fx else za_price.zar_amount_cents
                    local_currency = c_ref.currency if fx else "ZAR"
                    row = SubjectCountryPrice(
                        subject_id=subject.id,
                        country_code=country_code,
                        local_amount_cents=local_cents,
                        zar_amount_cents=za_price.zar_amount_cents,
                        local_currency=local_currency,
                        is_active=True
                    )
                    db.session.add(row)
                    db.session.commit()

        if row:
            price_ctx.update({
                "price_id": row.id,
                "country_code": row.country_code,
                "local_amount": row.local_amount_cents,
                "local_currency": row.local_currency,
                "estimated_zar": row.zar_amount_cents,
                "fx_rate": getattr(row, "fx_rate", None),
                "is_discount": getattr(row, "is_discount", False),
            })
            price_ctx["has_quote"] = True
        else:
            flash("No pricing found for that country yet.", "warning")

    countries = db.session.execute(
        sa_text("""
            SELECT r.alpha2 AS code, r.name
              FROM ref_country_currency r
             WHERE (r.is_active IS NULL OR r.is_active::text IN ('1','t','true','TRUE'))
             ORDER BY r.name
        """)
    ).mappings().all()

    if row:
        store_quote_baton(
            subject_slug,
            country_code,
            row.id,
            price_version=row.price_version,   # ✅ use the DB value
            local_currency=row.local_currency,
            local_amount_cents=row.local_amount_cents,
            zar_amount_cents=row.zar_amount_cents,
        )
        
        # We no longer auto-bypass for authenticated users.
        # They should see the quote page, and then click "Buy Now" or "Start Free Trial"
        # which will take them through the proper registration/authentication flow.
    return render_template(
        "payments/quote.html",
        subject=subject,
        subject_slug=subject_slug,
        next_url=next_url,
        countries=countries,
        price=price_ctx,
        commercial_mode=commercial_mode,
        trial_days=trial_days,
        requires_price=requires_price,
        trial_allowed=trial_allowed,
        local_currency=row.local_currency if row else None,
        local_amount_cents=row.local_amount_cents if row else None,
        zar_amount_cents=row.zar_amount_cents if row else None,
        country_code=row.country_code if row else country_code,
        is_upgrade=is_upgrade,
    )

def anchor_quote_to_baton(subject, country_code):
    """
    Ensure baton carries full quote information for a subject/country.
    Returns a dict with subject_slug, price_id, country_code,
    quoted_currency, quoted_amount_cents.
    """
    baton = get_baton_context() or {}

    price_row = SubjectCountryPrice.query.filter_by(
        subject_id=subject.id,
        country_code=country_code
    ).first()

    if price_row:
        baton["subject_slug"] = subject.slug
        baton["price_id"] = price_row.id
        baton["country_code"] = country_code
        baton["quoted_currency"] = price_row.currency
        baton["quoted_amount_cents"] = price_row.amount_cents

        # Persist baton back into session
        session["baton"] = baton

    return baton

def store_quote_baton(subject_slug, country_code, price_id,
                      enrollment=None, price_version=None,
                      local_currency=None, local_amount_cents=None,
                      zar_amount_cents=None):

    if enrollment:
        # Enrollment is authoritative
        session["subject_slug"] = subject_slug
        session["country_code"] = enrollment.country_code
        session["price_id"] = enrollment.price_id
        session["price_version"] = getattr(enrollment, "price_version", None)
        session["price_locked_at"] = getattr(enrollment, "price_locked_at", None)
        session["local_currency"] = getattr(enrollment, "local_currency", None)
        session["local_amount_cents"] = getattr(enrollment, "local_amount_cents", None)
        session["zar_amount_cents"] = getattr(enrollment, "zar_amount_cents", None)
    else:
        # Fresh quote wins if no enrollment yet
        session["subject_slug"] = subject_slug
        session["country_code"] = country_code
        session["price_id"] = price_id
        session["price_version"] = price_version
        session["price_locked_at"] = datetime.utcnow()
        session["local_currency"] = local_currency
        session["local_amount_cents"] = local_amount_cents
        session["zar_amount_cents"] = zar_amount_cents

    print(
        "Stored baton:",
        "subject_slug =", session.get("subject_slug"),
        "country_code =", session.get("country_code"),
        "price_id =", session.get("price_id"),
        "price_version =", session.get("price_version"),
        "price_locked_at =", session.get("price_locked_at"),
        "local_currency =", session.get("local_currency"),
        "local_amount_cents =", session.get("local_amount_cents"),
        "zar_amount_cents =", session.get("zar_amount_cents"),
    )

def get_baton_context():
    return {
        "subject_slug": session.get("subject_slug"),
        "country_code": session.get("country_code"),
        "price_id": session.get("price_id"),
        "price_version": session.get("price_version"),
        "price_locked_at": session.get("price_locked_at"),
        "local_currency": session.get("local_currency"),
        "local_amount_cents": session.get("local_amount_cents"),
        "zar_amount_cents": session.get("zar_amount_cents"),
    }

def get_baton_with_user_and_subject():
    baton = get_baton_context() or {}
    # Always carry current_user.id
    user_id = getattr(current_user, "id", None) or session.get("user_id")
    if user_id:
        baton["user_id"] = user_id

    # Preserve subject info if already in session
    subj_slug = session.get("subject_slug")
    subj_id = session.get("subject_id")
    if subj_slug and not baton.get("subject_slug"):
        baton["subject_slug"] = subj_slug
    if subj_id and not baton.get("subject_id"):
        baton["subject_id"] = subj_id

    return baton


def bind_baton_to_user(user_id: int, price_id: int | str):
    """
    Given a user ID and price_id, reconstruct the baton and bind it to the user.
    Stores:
      - subject_slug
      - country_code
      - price_id
      - user_id
    """
    row = SubjectCountryPrice.query.filter_by(id=price_id).first()
    if not row:
        return None

    subject = AuthSubject.query.filter_by(id=row.subject_id).first()
    if not subject:
        return None

    baton = {
        "user_id": user_id,
        "subject_slug": subject.slug,
        "country_code": row.country_code,
        "price_id": row.id,
    }

    session.update(baton)
    return baton

def get_enrollment_context(user_id, subject_id=None):
    if not user_id:
        return None

    query = UserEnrollment.query.filter_by(user_id=user_id, status="paid")
    if subject_id:
        query = query.filter_by(subject_id=subject_id)

    enr = query.order_by(UserEnrollment.id.asc()).first()
    if not enr:
        return None

    subj = AuthSubject.query.get(enr.subject_id)

    return {
        "subject_slug": subj.slug if subj else None,
        "price_id": enr.price_id,                  # ✅ include price_id
        "price_version": enr.price_version,
        "country_code": enr.country_code,
        "quoted_currency": enr.quoted_currency,
        "quoted_amount": (enr.quoted_amount_cents or 0) / 100,
        "charged_currency": enr.charged_currency,
        "charged_amount": (enr.charged_amount_cents or 0) / 100,
    }

def resolve_baton_from_price(price_id: int | str):
    row = SubjectCountryPrice.query.filter_by(id=price_id).first()
    if not row:
        return None

    subject = AuthSubject.query.filter_by(id=row.subject_id).first()
    if not subject:
        return None

    baton = {
        "subject_slug": subject.slug,
        "country_code": row.country_code,
        "price_id": row.id,
        "quoted_currency": row.local_currency,
        "quoted_amount_cents": row.local_amount_cents,
        "price_version": row.price_version,
        "price_locked_at": datetime.utcnow(),
        "local_currency": row.local_currency,
        "local_amount_cents": row.local_amount_cents,
        "zar_amount_cents": row.zar_amount_cents,
    }

    session.update(baton)
    return baton

def seed_subject_context(subj):
    session["subject_slug"] = subj.slug
    session["subject_id"] = subj.id
    return {
        "subj": subj,
        "subject_slug": subj.slug,
        "subject_id": subj.id
    }
