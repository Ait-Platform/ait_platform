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
            phone=form.phone.data
        )
        db.session.add(new_debtor)
        db.session.commit()
        flash("SOA Setup saved. Now configure financials.", "success")
        return redirect(url_for("debtors_bp.debtor_financials", debtor_id=new_debtor.id))
    return render_template("program_debtors/add_debtor.html", form=form)

@debtors_bp.route("/debtor/<int:debtor_id>/edit", methods=["GET", "POST"])
@login_required
def edit_debtor(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    form = DebtorForm(obj=debtor)
    
    if form.validate_on_submit():
        debtor.name = form.name.data
        debtor.email = form.email.data
        debtor.phone = form.phone.data
        
        db.session.commit()
        flash("SOA Profile updated successfully.", "success")
        return redirect(url_for('debtors_bp.dashboard'))
        
    return render_template('program_debtors/edit_debtor.html', form=form, debtor=debtor)

from app.program_debtors.forms import RecurringChargeForm, OpeningBalanceForm

@debtors_bp.route("/debtor/<int:debtor_id>/financials")
@login_required
def debtor_financials(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    charges = DebtorChargeMap.query.filter_by(debtor_id=debtor.id).all()
    
    rc_form = RecurringChargeForm()
    ob_form = OpeningBalanceForm()
    
    # Check if opening balance is already set in ledger
    has_opening_balance = DebtorLedger.query.filter_by(debtor_id=debtor.id, ref='OPENING').first() is not None
    
    return render_template('program_debtors/debtor_financials.html', debtor=debtor, charges=charges, rc_form=rc_form, ob_form=ob_form, has_opening_balance=has_opening_balance)

@debtors_bp.route("/debtor/<int:debtor_id>/add_charge", methods=["POST"])
@login_required
def add_recurring_charge(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    form = RecurringChargeForm()
    
    if form.validate_on_submit():
        cmap = DebtorChargeMap(
            debtor_id=debtor.id,
            charge_description=form.charge_description.data,
            amount=int(round(form.charge_amount.data * 100)),
            frequency=form.charge_frequency.data,
            day_of_month=form.day_of_month.data
        )
        db.session.add(cmap)
        db.session.commit()
        flash("Recurring charge added successfully.", "success")
    return redirect(url_for('debtors_bp.debtor_financials', debtor_id=debtor.id))

@debtors_bp.route("/debtor/<int:debtor_id>/delete_charge/<int:charge_id>", methods=["POST"])
@login_required
def delete_recurring_charge(debtor_id, charge_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    cmap = DebtorChargeMap.query.filter_by(id=charge_id, debtor_id=debtor.id).first_or_404()
    
    db.session.delete(cmap)
    db.session.commit()
    flash("Recurring charge deleted.", "success")
    return redirect(url_for('debtors_bp.debtor_financials', debtor_id=debtor.id))

@debtors_bp.route("/debtor/<int:debtor_id>/add_opening_balance", methods=["POST"])
@login_required
def add_opening_balance(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    form = OpeningBalanceForm()
    
    if form.validate_on_submit():
        if form.opening_balance.data > 0:
            ledger = DebtorLedger(
                debtor_id=debtor.id,
                description="Opening Balance",
                kind="debit",
                amount=int(round(form.opening_balance.data * 100)),
                ref="OPENING",
                txn_date=form.txn_date.data
            )
            db.session.add(ledger)
            db.session.commit()
            flash("Opening balance set successfully.", "success")
    return redirect(url_for('debtors_bp.debtor_financials', debtor_id=debtor.id))

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
            amount=int(round(form.amount.data * 100)),
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

@debtors_bp.route("/transaction/<int:txn_id>/edit", methods=["GET", "POST"])
@login_required
def edit_transaction(txn_id):
    txn = DebtorLedger.query.get_or_404(txn_id)
    debtor = Debtor.query.filter_by(id=txn.debtor_id, user_id=current_user.id).first_or_404()
    
    form = TransactionForm(obj=txn)
    
    if request.method == "GET":
        form.amount.data = txn.amount / 100.0
    
    if form.validate_on_submit():
        txn.txn_date = form.txn_date.data
        txn.description = form.description.data
        txn.kind = form.kind.data
        txn.ref = form.ref.data
        txn.amount = int(round(form.amount.data * 100))
        
        db.session.commit()
        flash("Transaction updated successfully.", "success")
        return redirect(url_for("debtors_bp.debtor_view", debtor_id=debtor.id))
        
    return render_template("program_debtors/edit_transaction.html", form=form, txn=txn)

from flask import send_file
import io
from app.utils.pdf_render import html_to_pdf_bytes
from flask import current_app

@debtors_bp.route("/soa_redirect")
@login_required
def soa_redirect():
    debtor_id = request.args.get('debtor_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor_id, start_date=start_date, end_date=end_date, pdf=1))

@debtors_bp.route("/debtor/<int:debtor_id>/soa")
@login_required
def generate_soa(debtor_id):
    from datetime import datetime
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    profile = SoaProfile.query.filter_by(user_id=current_user.id).first()
    
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    
    running_balance = 0
    period_ledgers = []
    ledgers = DebtorLedger.query.filter_by(debtor_id=debtor.id).order_by(DebtorLedger.txn_date, DebtorLedger.id).all()
    
    period_opening_balance = 0
    calculated_opening_yet = False
    
    for l in ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        else:
            running_balance -= l.amount
            
        l.running_balance = running_balance
        
        if start_date and l.txn_date < start_date:
            continue
            
        if not calculated_opening_yet:
            # The opening balance for this period is the running balance just BEFORE this transaction
            prev_balance = running_balance - l.amount if l.kind == 'debit' else running_balance + l.amount
            period_opening_balance = prev_balance
            calculated_opening_yet = True
            
        if end_date and l.txn_date > end_date:
            continue
            
        period_ledgers.append(l)
        
    if not calculated_opening_yet:
        period_opening_balance = running_balance
        
    html_content = render_template("program_debtors/soa_template.html", 
                                   debtor=debtor, 
                                   ledgers=period_ledgers, 
                                   profile=profile, 
                                   running_balance=running_balance,
                                   period_opening_balance=period_opening_balance,
                                   start_date=start_date,
                                   end_date=end_date)
    
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
