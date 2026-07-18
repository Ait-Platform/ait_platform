from flask import Blueprint, redirect, render_template, url_for
from flask_login import login_required

tpx_bp = Blueprint(
    "tpx_bp",
    __name__,
    template_folder="templates"
)

@tpx_bp.route("/welcome")
def welcome():
    return render_template("program_tpx/welcome.html")

@tpx_bp.route("/about")
def about():
    return render_template("program_tpx/about.html")

@tpx_bp.route("/how-it-works")
def how_it_works():
    return render_template("program_tpx/how_it_works.html")

@tpx_bp.route("/register")
def register_choice():
    return redirect(url_for("auth_bp.register"))

@tpx_bp.route("/pricing")
def pricing():
    from flask import request, render_template
    from app.payments.pricing import price_for_country
    from app.models.auth import AuthSubject
    
    country_code = request.headers.get("CF-IPCountry", "ZA")
    tpx_subject = AuthSubject.query.filter_by(slug='tpx').first()
    
    display_price = "ZAR 100"
    if tpx_subject:
        row = price_for_country(tpx_subject.id, country_code)
        if row:
            # row returns (local_amount_cents, zar_amount_cents, currency)
            local_amt = row[0] / 100.0
            currency = row[2]
            display_price = f"{currency} {local_amt:,.2f}"
            
    return render_template("program_tpx/pricing.html", display_price=display_price)

#price = price_for_country(subject_id, country_code)'quote_bp.quote', subject='tpx'

@tpx_bp.route("/quote")
@login_required
def quote():
    return render_template("program_tpx/quote.html")

@tpx_bp.route("/dashboard")
@login_required
def dashboard():
    return render_template("program_tpx/dashboard.html")







