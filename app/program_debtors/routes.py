from flask import render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from app.program_debtors import debtors_bp
from app.models.auth import AuthSubject, UserEnrollment
from app.models.debtors import SoaProfile, Debtor, DebtorLedger, DebtorChargeMap, DebtorsWallet
from app.extensions import db
from app.program_debtors.forms import SoaProfileForm, DebtorForm

@debtors_bp.route("/about")
def about():
    """Public about page for the Debtors module"""
    return render_template("program_debtors/about.html")

@debtors_bp.route("/router")
@login_required
def debtors_router():
    user_id = current_user.id
    
    # Check enrollment
    subject = AuthSubject.query.filter_by(slug='debtors').first()
    if subject:
        enrollment = UserEnrollment.query.filter_by(user_id=user_id, subject_id=subject.id).first()
    else:
        enrollment = None
        
    if not enrollment or enrollment.status == "pending":
        flash("You must complete registration before accessing Debtors.", "warning")
        return redirect(url_for("auth_bp.register_decision", subject="debtors"))
        
    return redirect(url_for("debtors_bp.dashboard"))

@debtors_bp.route("/dashboard")
@login_required
def dashboard():
    wallet = DebtorsWallet.query.filter_by(user_id=current_user.id).first()
    debtors = Debtor.query.filter_by(user_id=current_user.id, is_active=True).all()
    
    # Calculate balances for each debtor dynamically (or we could store it)
    for d in debtors:
        total_debits = sum(l.amount for l in d.ledgers if l.kind == 'debit')
        total_credits = sum(l.amount for l in d.ledgers if l.kind == 'credit')
        d.current_balance = total_debits - total_credits
        
    return render_template("program_debtors/dashboard.html", wallet=wallet, debtors=debtors)

@debtors_bp.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    profile_record = SoaProfile.query.filter_by(user_id=current_user.id).first()
    form = SoaProfileForm(obj=profile_record)
    
    if form.validate_on_submit():
        if not profile_record:
            profile_record = SoaProfile(user_id=current_user.id)
            db.session.add(profile_record)
            
        form.populate_obj(profile_record)
        db.session.commit()
        flash("SOA Profile updated successfully.", "success")
        return redirect(url_for("debtors_bp.dashboard"))
        
    return render_template("program_debtors/profile.html", form=form)

@debtors_bp.route("/add_debtor", methods=["GET", "POST"])
@login_required
def add_debtor():
    form = DebtorForm()
    
    if form.validate_on_submit():
        new_debtor = Debtor(
            user_id=current_user.id,
            name=form.name.data,
            email=form.email.data,
            phone=form.phone.data,
            opening_balance=form.opening_balance.data
        )
        db.session.add(new_debtor)
        db.session.flush() # get ID
        
        # Add opening balance ledger entry if > 0
        if new_debtor.opening_balance > 0:
            ledger = DebtorLedger(
                debtor_id=new_debtor.id,
                description="Opening Balance",
                kind="debit",
                amount=new_debtor.opening_balance,
                ref="OPENING"
            )
            db.session.add(ledger)
            
        # Add charge map if provided
        if form.charge_description.data and form.charge_amount.data > 0:
            cmap = DebtorChargeMap(
                debtor_id=new_debtor.id,
                charge_description=form.charge_description.data,
                amount=form.charge_amount.data,
                frequency=form.charge_frequency.data
            )
            db.session.add(cmap)
            
        db.session.commit()
        flash("Debtor added successfully.", "success")
        return redirect(url_for("debtors_bp.dashboard"))
        
    return render_template("program_debtors/add_debtor.html", form=form)

from app.program_debtors.forms import TransactionForm

@debtors_bp.route("/debtor/<int:debtor_id>")
@login_required
def debtor_view(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    # Calculate running balance per ledger entry
    running_balance = 0
    # we need ledgers sorted by date then id (to maintain order for same date)
    ledgers = DebtorLedger.query.filter_by(debtor_id=debtor.id).order_by(DebtorLedger.txn_date, DebtorLedger.id).all()
    
    for l in ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        else:
            running_balance -= l.amount
        l.running_balance = running_balance
        
    debtor.current_balance = running_balance
    
    form = TransactionForm() # for inline addition
    return render_template("program_debtors/debtor_view.html", debtor=debtor, ledgers=ledgers, form=form)

@debtors_bp.route("/debtor/<int:debtor_id>/add_transaction", methods=["POST"])
@login_required
def add_transaction(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    form = TransactionForm()
    
    if form.validate_on_submit():
        txn = DebtorLedger(
            debtor_id=debtor.id,
            txn_date=form.txn_date.data,
            description=form.description.data,
            kind=form.kind.data,
            amount=form.amount.data,
            ref=form.ref.data
        )
        db.session.add(txn)
        db.session.commit()
        flash("Transaction added successfully.", "success")
    else:
        for field, errors in form.errors.items():
            for error in errors:
                flash(f"Error in {getattr(form, field).label.text}: {error}", "danger")
                
    return redirect(url_for("debtors_bp.debtor_view", debtor_id=debtor.id))

from flask import send_file
import io
from app.utils.pdf_render import html_to_pdf_bytes
from flask import current_app

@debtors_bp.route("/debtor/<int:debtor_id>/soa")
@login_required
def generate_soa(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    profile = SoaProfile.query.filter_by(user_id=current_user.id).first()
    
    # Optional: Date filtering could go here via request.args
    
    running_balance = 0
    ledgers = DebtorLedger.query.filter_by(debtor_id=debtor.id).order_by(DebtorLedger.txn_date, DebtorLedger.id).all()
    
    for l in ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        else:
            running_balance -= l.amount
        l.running_balance = running_balance
        
    html_content = render_template("program_debtors/soa_template.html", debtor=debtor, ledgers=ledgers, profile=profile, running_balance=running_balance)
    
    if request.args.get('pdf') == '1':
        try:
            pdf_bytes = html_to_pdf_bytes(html_content, base_url=request.host_url)
            return send_file(
                io.BytesIO(pdf_bytes),
                mimetype='application/pdf',
                as_attachment=True,
                download_name=f"SOA_{debtor.name.replace(' ', '_')}.pdf"
            )
        except Exception as e:
            current_app.logger.error(f"Failed to generate SOA PDF: {e}")
            flash("Error generating PDF. Please try again or print the page directly.", "danger")
            return html_content
            
    return html_content
