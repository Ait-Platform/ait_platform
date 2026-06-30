from flask import render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from app.extensions import db
from . import mechanic_bp
from app.models.mechanic import MechClient, MechVehicle, MechJobCard, MechInvoice

@mechanic_bp.route("/mechanic/dashboard")
@login_required
def mechanic_dashboard():
    # Placeholder for the mechanic dashboard
    # Will display active job cards, recent invoices, and quick actions
    job_cards = MechJobCard.query.order_by(MechJobCard.created_at.desc()).limit(10).all()
    return render_template("program_mechanic/dashboard.html", job_cards=job_cards)

@mechanic_bp.route("/mechanic/intake", methods=["GET", "POST"])
@login_required
def mechanic_intake():
    # Placeholder for new vehicle intake
    if request.method == "POST":
        # Handle form submission for new client + vehicle
        flash("Vehicle intake successful (Mock)", "success")
        return redirect(url_for('mechanic_bp.mechanic_dashboard'))
    return render_template("program_mechanic/intake.html")

@mechanic_bp.route("/mechanic/job/<int:id>", methods=["GET", "POST"])
@login_required
def job_card_detail(id):
    job_card = MechJobCard.query.get_or_404(id)
    return render_template("program_mechanic/job_card.html", job_card=job_card)

@mechanic_bp.route("/mechanic/invoice/<int:id>")
@login_required
def generate_invoice(id):
    # Logic to calculate totals from labor/parts and generate MechInvoice
    job_card = MechJobCard.query.get_or_404(id)
    return render_template("program_mechanic/invoice_view.html", job_card=job_card)
