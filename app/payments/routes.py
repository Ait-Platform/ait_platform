from flask import request, render_template, redirect, url_for, flash
from sqlalchemy import text

from app.extensions import db
from app.models.auth import AuthSubject
from . import payment_bp


@payment_bp.get("/pricing/base", endpoint="pricing_base")
def pricing_base():
    subject_slug = request.args.get("subject", "loss")

    subject = AuthSubject.query.filter_by(slug=subject_slug).first_or_404()
    all_subjects = AuthSubject.query.order_by(AuthSubject.name).all()

    rows = db.session.execute(
        text("""
            SELECT
                currency,
                amount_cents,
                plan,
                COALESCE(is_active, 1) AS is_active
            FROM auth_pricing
            WHERE subject_id = :sid
            ORDER BY
                COALESCE(active_from, created_at) DESC,
                COALESCE(updated_at, created_at) DESC,
                id DESC
        """),
        {"sid": subject.id},
    ).mappings().all()

    return render_template(
        "admin_general/pricing_base.html",
        subject=subject,
        all_subjects=all_subjects,
        prices=rows,
    )


@payment_bp.route("/pricing", methods=["GET", "POST"], endpoint="pricing_index")
def pricing_index():
    subject_slug = request.args.get("subject", "loss")

    subject = AuthSubject.query.filter_by(slug=subject_slug).first_or_404()
    all_subjects = AuthSubject.query.order_by(AuthSubject.name).all()

    anchor_row = db.session.execute(
        text("""
            SELECT amount_cents
              FROM auth_pricing
             WHERE subject_id = :sid
               AND plan = 'enrollment'
               AND COALESCE(is_active, 1) = 1
               AND (active_to IS NULL OR active_to > CURRENT_TIMESTAMP)
             ORDER BY
               COALESCE(active_from, created_at) DESC,
               COALESCE(updated_at, created_at) DESC,
               id DESC
             LIMIT 1
        """),
        {"sid": subject.id},
    ).first()

    anchor_zar_cents = (
        int(anchor_row.amount_cents)
        if anchor_row and anchor_row.amount_cents is not None
        else None
    )

    countries = db.session.execute(
        text("""
            SELECT
                alpha2     AS country_code,
                currency   AS currency_code,
                name,
                fx_to_zar
            FROM ref_country_currency
            WHERE COALESCE(is_active, TRUE) = TRUE
            ORDER BY name ASC
        """)
    ).mappings().all()

    if request.method == "POST":
        country_code = (request.form.get("country_code") or "").strip().upper()
        fx_rate_raw  = (request.form.get("fx_rate") or "").strip()
        local_raw    = (request.form.get("local_amount_cents") or "").strip()

        if not country_code or not anchor_zar_cents:
            flash("Country code and a ZAR anchor are required.", "error")
            return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))

        if not fx_rate_raw:
            for c in countries:
                if c["country_code"] == country_code and c.get("fx_to_zar") is not None:
                    fx_rate_raw = str(c["fx_to_zar"])
                    break

        local_amount_cents = None

        if local_raw:
            try:
                local_amount_cents = int(local_raw)
            except ValueError:
                flash("Local amount must be an integer number of cents.", "error")
                return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))

        elif fx_rate_raw:
            try:
                fx_rate = float(fx_rate_raw)
                local_amount_cents = int(round(anchor_zar_cents * fx_rate))
            except ValueError:
                flash("FX rate must be a number.", "error")
                return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))
        else:
            flash("Either enter a local amount or an FX rate.", "error")
            return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))

        base_raw = (request.form.get("base_zar_cents") or "").strip()
        if not base_raw:
            flash("Please enter a base ZAR amount (cents).", "error")
            return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))

        try:
            base_zar_cents = int(base_raw)
        except ValueError:
            flash("Base ZAR amount must be whole cents (integer).", "error")
            return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))

        # Enforce minimum ZAR floor to protect against extreme currency devaluation
        MIN_ZAR_CENTS = 5000  # R50.00 ZAR minimum
        if base_zar_cents < MIN_ZAR_CENTS:
            flash(f"Error: The calculated ZAR value (R{base_zar_cents/100:.2f}) is below the absolute minimum of R{MIN_ZAR_CENTS/100:.2f}.", "error")
            return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))

        db.session.execute(
            text("""
                INSERT INTO subject_country_price
                    (subject_id, country_code, local_amount_cents, zar_amount_cents, is_active)
                VALUES
                    (:sid, :cc, :local, :zar, TRUE)
                ON CONFLICT (subject_id, country_code)
                DO UPDATE SET
                    local_amount_cents = EXCLUDED.local_amount_cents,
                    zar_amount_cents   = EXCLUDED.zar_amount_cents,
                    is_active          = EXCLUDED.is_active
            """),
            {
                "sid": subject.id,
                "cc": country_code,
                "local": int(local_amount_cents),
                "zar": int(base_zar_cents),
            },
        )
        db.session.commit()
        return redirect(url_for("payment_bp.pricing_index", subject=subject.slug))

    rows = db.session.execute(
        text("""
            SELECT
                country_code,
                local_amount_cents,
                zar_amount_cents,
                COALESCE(is_active, TRUE) AS is_active
            FROM subject_country_price
            WHERE subject_id = :sid
            ORDER BY country_code ASC
        """),
        {"sid": subject.id},
    ).mappings().all()

    return render_template(
        "admin_general/pricing_index.html",
        subject=subject,
        all_subjects=all_subjects,
        tiers=rows,
        anchor_zar_cents=anchor_zar_cents,
        countries=countries,
    )

@payment_bp.route("/wallet/topup/<subject_slug>")
def wallet_topup(subject_slug):
    from flask import session
    from flask_login import current_user, login_required
    if not current_user.is_authenticated:
        return redirect(url_for("auth_bp.login"))
        
    from app.models.auth import AitTokenWallet, AuthSubject, UserEnrollment
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    
    auth_subject = AuthSubject.query.filter_by(slug=subject_slug).first()
    if not auth_subject:
        flash(f"Invalid module: {subject_slug}", "error")
        return redirect("/")
        
    currency = "ZAR"
    currency_symbol = "R"
    price_cents = 10000
    
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=auth_subject.id).first()
    if enrollment:
        country_code = enrollment.country_code or session.get("country_code", "")
        
        # Fetch the CURRENT live price from SubjectCountryPrice
        from app.enrollment.logic import get_quote_for_subject_country
        quote = get_quote_for_subject_country(auth_subject.id, country_code)
        
        if quote:
            currency = quote.local_currency
            price_cents = quote.local_amount_cents
        elif enrollment.local_currency:
            currency = enrollment.local_currency
            price_cents = enrollment.local_amount_cents
            
    if currency == "USD": currency_symbol = "$"
    elif currency == "GBP": currency_symbol = "£"
    elif currency == "EUR": currency_symbol = "€"
    elif currency == "NGN": currency_symbol = "₦"
    elif currency == "GHS": currency_symbol = "₵"
    elif currency == "KES": currency_symbol = "KSh "
    elif currency == "RWF": currency_symbol = "FRw "
    elif currency != "ZAR": currency_symbol = ""
    
    price_display = f"{price_cents / 100:.2f}"
            
    return render_template("wallet_topup.html", wallet=wallet, currency=currency, currency_symbol=currency_symbol, price_display=price_display, price_cents=price_cents, subject_slug=subject_slug)

@payment_bp.route("/wallet/checkout", methods=["POST"])
def wallet_checkout():
    from flask import session
    from flask_login import current_user
    if not current_user.is_authenticated:
        return redirect(url_for("auth_bp.login"))
        
    tokens = 100
    price_cents = int(request.form.get("price_cents", 10000))
    currency = request.form.get("currency", "ZAR")
    subject_slug = request.form.get("subject_slug", "debtors")
            
    session["topup_amount_cents"] = price_cents
    session["topup_currency"] = currency
    session["topup_tokens"] = tokens
    
    # Send directly to the unified Paystack handler using the generic format
    checkout_subject = f"{subject_slug}_topup"
    
    return redirect(url_for('paystack_bp.paystack_start', subject=checkout_subject, email=current_user.email))
