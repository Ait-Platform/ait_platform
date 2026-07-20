from flask import render_template, redirect, url_for, flash, request, session
from flask_login import login_required, current_user
from app.extensions import db
from . import debtors_bp
from app.models.auth import AuthSubject, UserEnrollment
from app.models.debtors import SoaProfile, DebtorsWallet
import json

@debtors_bp.route("/about")
def about():
    # Public facing about page
    return render_template("program_debtors/about.html")

@debtors_bp.route("/")
@login_required
def debtors_router():
    user_id = current_user.id

    # 1. Resolve enrollment
    subject = AuthSubject.query.filter_by(slug='debtors').first()
    if subject:
        enrollment = UserEnrollment.query.filter_by(user_id=user_id, subject_id=subject.id).first()
    else:
        enrollment = None
        
    if not enrollment or enrollment.status == "pending":
        flash("You must complete payment or apply a voucher before accessing Debtors.", "warning")
        return redirect(url_for("auth_bp.register_decision", subject="debtors"))

    # 2. Check for wallet (created during voucher redemption or Yoco payment)
    wallet = DebtorsWallet.query.filter_by(user_id=user_id).first()
    if not wallet:
        wallet = DebtorsWallet(user_id=user_id, balance=0)
        db.session.add(wallet)
        db.session.commit()

    return redirect(url_for("debtors_bp.dashboard"))

@debtors_bp.route("/dashboard")
@login_required
def dashboard():
    wallet = DebtorsWallet.query.filter_by(user_id=current_user.id).first()
    balance = wallet.balance if wallet else 0
    return render_template("program_debtors/dashboard.html", token_balance=balance)
