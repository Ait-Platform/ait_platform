from decimal import Decimal

from flask import Blueprint, flash, redirect, render_template, request, url_for, session
from flask_login import current_user, login_required

from app.models.spv import Spv, SpvCommitment, SpvDeal, SpvInvestor, SpvSection, SpvTransaction
#from app.services import calculate_investor_roi, calculate_spv_equity, get_spv_funding_status
from app.extensions import db
from app.program_spv.helpers import calculate_investor_roi, calculate_spv_equity, get_spv_funding_status

spv_bp = Blueprint("spv_bp", __name__)


@spv_bp.route("/program/spv/<code>")
def spv_dashboard(code):
    spv = Spv.query.filter_by(code=code).first_or_404()
    return render_template("program_spv/dashboard.html", spv=spv)
 
@spv_bp.route("/program/spv/<code>/invest", methods=["POST"])
def invest(code):
    spv = Spv.query.filter_by(code=code).first_or_404()

    amount = Decimal(request.form.get("amount"))

    investor = SpvInvestor.query.filter_by(
        spv_id=spv.id,
        user_id=current_user.id
    ).first()

    if not investor:
        investor = SpvInvestor(
            spv_id=spv.id,
            user_id=current_user.id
        )
        db.session.add(investor)

    investor.amount_invested += amount

    tx = SpvTransaction(
        spv_id=spv.id,
        investor=investor,
        amount=amount,
        transaction_type="investment"
    )

    db.session.add(tx)
    db.session.commit()

    calculate_spv_equity(spv.id)

    return redirect(url_for("spv.spv_dashboard", code=code))

@spv_bp.route("/program/spv/about")
def about_spv():
    return render_template("program_spv/about.html")

@spv_bp.route("/program/spv/price")
def price_spv():
    return render_template("program_spv/price.html")

@spv_bp.route("/program/spv")
def spv_list():
    spvs = Spv.query.all()
    return render_template("program_spv/list.html", spvs=spvs)

@spv_bp.route("/program/spv/investor")
@login_required
def investor_dashboard():
    from app.models.spv import SpvParticipation
    
    # Get user's confirmed investments
    investments = SpvParticipation.query.filter_by(
        user_id=current_user.id,
        status="confirmed"
    ).order_by(SpvParticipation.created_at.desc()).all()
    
    # Calculate total invested
    total_invested = sum(inv.amount for inv in investments) if investments else 0
    
    if not investments:
        from flask import flash, redirect, url_for
        flash("You must complete your R100 ZAR initial commitment to access your investor dashboard.", "warning")
        return redirect(url_for("yoco_bp.yoco_start", email=current_user.email, subject="spv_registration", debug=0))
    
    # Fetch Dale SPV as an available opportunity
    dale_deal = SpvDeal.query.filter_by(slug="dale-housing").first()

    return render_template(
        "program_spv/investor_dashboard.html",
        investments=investments,
        total_invested=total_invested,
        dale_deal=dale_deal
    )

@spv_bp.route("/program/spv/<code>/deal")
def spv_deal_page(code):
    spv = Spv.query.filter_by(code=code).first_or_404()
    #return render_template("program_spv/deal.html", spv=spv, financials=spv.financial_model)
    return render_template(
        "program_spv/deal.html",
        spv=spv
    )

@spv_bp.route("/program/spv/<code>/commit", methods=["GET", "POST"])
@login_required
def commit_to_spv(code):
    spv = Spv.query.filter_by(code=code).first_or_404()

    if request.method == "POST":
        amount = request.form.get("amount")
        # save commitment here
        return redirect(url_for("spv_bp.spv_deal_page", code=code))

    return render_template("program_spv/commit.html", spv=spv)

@spv_bp.route("/investments")
def investments():

    return render_template(
        "program_spv/investments.html"
    )

@spv_bp.route("/almond-dale")
def almond_dale_spv():

    deals = SpvDeal.query.filter_by(slug="dale-housing").all()

    return render_template(
        "program_spv/almond_dale_spv.html",
        deals=deals
    )

@spv_bp.route("/portfolio/<slug>/<section>")
def portfolio_section(slug, section):

    spv = SpvDeal.query.filter_by(
        slug=slug
    ).first_or_404()

    return render_template(
        "program_spv/portfolio_section.html",
        spv=spv,
        section=section
    )

@spv_bp.route("/portfolio/<slug>/email-brochure", methods=["POST"])
def email_brochure(slug):

    deal = SpvDeal.query.filter_by(
        slug=slug
    ).first_or_404()

    email = request.form.get("email")

    flash(
        f"Brochure request received for {email}",
        "success"
    )

    return redirect(
        url_for(
            "spv_bp.portfolio_detail",
            slug=slug
        )
    )

@spv_bp.route("/portfolio/<slug>/invest", methods=["POST"])
@login_required
def initiate_spv_investment(slug):
    deal = SpvDeal.query.filter_by(slug=slug).first_or_404()
    amount = float(request.form.get("amount", 0))
    pseudonym = request.form.get("pseudonym", "Anonymous").strip()
    
    if amount < 100:
        flash("Minimum investment is ZAR 100", "error")
        return redirect(url_for("spv_bp.portfolio_detail", slug=slug) + "#invest")
        
    from app.models.spv import SpvParticipation
    existing = SpvParticipation.query.filter_by(deal_id=deal.id, pseudonym=pseudonym).first()
    if existing:
        flash(f"The pseudonym '{pseudonym}' is already in use by another investor on this SPV. Please choose a different one.", "error")
        return redirect(url_for("spv_bp.portfolio_detail", slug=slug) + "#invest")
        
    session["spv_pseudonym"] = pseudonym
    session["spv_amount"] = amount
    session["spv_deal_slug"] = slug
    session["zar_amount_cents"] = int(amount * 100)
    session["subject_slug"] = "spv_investment"
    
    return redirect(url_for("yoco_bp.yoco_start", subject="spv_investment", email=current_user.email))

@spv_bp.route("/portfolio/<slug>")
def portfolio_detail(slug):

    deal = SpvDeal.query.filter_by(
        slug=slug
    ).first_or_404()

    section_list = SpvSection.query.filter_by(
        deal_id=deal.id
    ).order_by(
        SpvSection.sort_order
    ).all()

    sections = {
        section.slug: section
        for section in section_list
    }

    return render_template(
        "program_spv/portfolio_detail.html",
        deal=deal,
        sections=sections
    )

@spv_bp.route("/ledger")
@login_required
def spv_ledger():
    from app.models.spv import SpvDeal, SpvParticipation
    # 1. Ensure user is an active investor
    user_investments = SpvParticipation.query.filter_by(
        user_id=current_user.id, status="confirmed"
    ).all()
    
    if not user_investments:
        from flask import flash, redirect, url_for
        flash("You must complete your R100 ZAR initial commitment to view the ledger.", "warning")
        return redirect(url_for("yoco_bp.yoco_start", email=current_user.email, subject="spv_registration", debug=0))

    # 2. Grab all participations for Dale SPV
    dale_deal = SpvDeal.query.filter_by(slug="dale-housing").first()
    all_participations = SpvParticipation.query.filter_by(
        deal_id=dale_deal.id, status="confirmed"
    ).order_by(SpvParticipation.created_at.asc()).all()

    # 3. Calculate total
    total_pool = sum(p.amount for p in all_participations)

    return render_template(
        "program_spv/ledger.html",
        participations=all_participations,
        total_pool=total_pool,
        dale_deal=dale_deal
    )


@spv_bp.route("/ledger/pseudonym", methods=["POST"])
@login_required
def set_pseudonym():
    from app.models.spv import SpvParticipation
    new_pseudo = request.form.get("pseudonym", "").strip()
    if new_pseudo:
        participations = SpvParticipation.query.filter_by(user_id=current_user.id).all()
        for p in participations:
            p.pseudonym = new_pseudo
        db.session.commit()
        from flask import flash
        flash("Pseudonym updated successfully.", "success")
    return redirect(url_for("spv_bp.spv_ledger"))

