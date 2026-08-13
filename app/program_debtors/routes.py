import os
import time
from werkzeug.utils import secure_filename
from flask import render_template, redirect, url_for, flash, request, current_app, session
from flask_login import login_required, current_user
from app.program_debtors import debtors_bp
from app.models.auth import AuthSubject, UserEnrollment
from app.models.debtors import SoaProfile, Debtor, DebtorLedger, DebtorChargeMap, BusinessBankAccount, SenderProfile
from app.models.auth import AitTokenWallet, AitTokenTransaction
from app.extensions import db
from app.program_debtors.forms import SoaProfileForm, DebtorForm, BankAccountForm, SenderProfileForm

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
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    if not wallet:
        from app.models.billing import SignupBonus
        bonus = SignupBonus.query.filter_by(program_slug='debtors').first()
        if not bonus:
            bonus = SignupBonus(program_slug='debtors', bonus_tokens=100)
            db.session.add(bonus)
            db.session.flush()
            
        wallet = AitTokenWallet(user_id=current_user.id, balance=bonus.bonus_tokens)
        db.session.add(wallet)
        db.session.flush()
        
        if bonus.bonus_tokens > 0:
            tx = AitTokenTransaction(
                wallet_id=wallet.id,
                amount=bonus.bonus_tokens,
                description="Initial Signup Bonus (Debtors)"
            )
            db.session.add(tx)
        db.session.commit()
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
            
        db.session.commit()
        flash("Global settings updated successfully.", "success")
        return redirect(url_for("debtors_bp.profile"))
        
    bank_accounts = BusinessBankAccount.query.filter_by(user_id=current_user.id).order_by(BusinessBankAccount.created_at.desc()).all()
    bank_form = BankAccountForm()
    
    sender_profiles = SenderProfile.query.filter_by(user_id=current_user.id).order_by(SenderProfile.created_at.desc()).all()
    sender_form = SenderProfileForm()
        
    return render_template("program_debtors/profile.html", form=form, profile=profile_record, 
                           bank_accounts=bank_accounts, bank_form=bank_form, 
                           sender_profiles=sender_profiles, sender_form=sender_form)

@debtors_bp.route("/add_sender_profile", methods=["POST"])
@login_required
def add_sender_profile():
    form = SenderProfileForm()
    if form.validate_on_submit():
        # Unset previous default if this is default
        if form.is_default.data:
            SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).update({'is_default': False})
            
        new_sp = SenderProfile(
            user_id=current_user.id,
            business_name=form.business_name.data,
            address=form.address.data,
            phone=form.phone.data,
            email=form.email.data,
            is_default=form.is_default.data
        )
        
        logo_file = form.logo_file.data
        if logo_file:
            filename = secure_filename(f"debtor_{current_user.id}_{int(time.time())}_{logo_file.filename}")
            upload_folder = os.path.join(current_app.root_path, "static", "uploads", "debtors")
            os.makedirs(upload_folder, exist_ok=True)
            logo_file.save(os.path.join(upload_folder, filename))
            new_sp.logo_url = filename
            
        db.session.add(new_sp)
        db.session.commit()
        flash("Sender profile added successfully.", "success")
    return redirect(url_for("debtors_bp.profile"))

@debtors_bp.route("/set_default_sender_profile/<int:profile_id>", methods=["POST"])
@login_required
def set_default_sender_profile(profile_id):
    SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).update({'is_default': False})
    sp = SenderProfile.query.filter_by(id=profile_id, user_id=current_user.id).first_or_404()
    sp.is_default = True
    db.session.commit()
    flash("Default sender profile updated.", "success")
    return redirect(url_for("debtors_bp.profile"))

@debtors_bp.route("/delete_sender_profile/<int:profile_id>", methods=["POST"])
@login_required
def delete_sender_profile(profile_id):
    sp = SenderProfile.query.filter_by(id=profile_id, user_id=current_user.id).first_or_404()
    
    # Check if assigned to any debtors
    if sp.debtors.count() > 0:
        flash("Cannot delete this profile because it is currently assigned to one or more debtors.", "danger")
        return redirect(url_for("debtors_bp.profile"))
        
    db.session.delete(sp)
    db.session.commit()
    flash("Sender profile deleted.", "success")
    return redirect(url_for("debtors_bp.profile"))

@debtors_bp.route("/add_bank_account", methods=["POST"])
@login_required
def add_bank_account():
    form = BankAccountForm()
    if form.validate_on_submit():
        if form.is_default.data:
            BusinessBankAccount.query.filter_by(user_id=current_user.id).update({'is_default': False})
            
        new_acc = BusinessBankAccount(
            user_id=current_user.id,
            bank_name=form.bank_name.data,
            account_name=form.account_name.data,
            account_number=form.account_number.data,
            bsb_branch=form.bsb_branch.data,
            swift_code=form.swift_code.data,
            is_default=form.is_default.data
        )
        
        if BusinessBankAccount.query.filter_by(user_id=current_user.id).count() == 0:
            new_acc.is_default = True
            
        db.session.add(new_acc)
        db.session.commit()
        flash("Bank account added successfully.", "success")
    return redirect(url_for("debtors_bp.profile"))

@debtors_bp.route("/bank_account/<int:account_id>/set_default", methods=["POST"])
@login_required
def set_default_bank_account(account_id):
    acc = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    BusinessBankAccount.query.filter_by(user_id=current_user.id).update({'is_default': False})
    acc.is_default = True
    db.session.commit()
    flash("Default bank account updated.", "success")
    return redirect(url_for("debtors_bp.profile"))

@debtors_bp.route("/bank_account/<int:account_id>/delete", methods=["POST"])
@login_required
def delete_bank_account(account_id):
    acc = BusinessBankAccount.query.filter_by(id=account_id, user_id=current_user.id).first_or_404()
    db.session.delete(acc)
    db.session.commit()
    flash("Bank account deleted.", "success")
    return redirect(url_for("debtors_bp.profile"))

@debtors_bp.route("/add_debtor", methods=["GET", "POST"])
@login_required
def add_debtor():
    form = DebtorForm()
    bank_accounts = BusinessBankAccount.query.filter_by(user_id=current_user.id).all()
    form.bank_account_id.choices = [(0, 'Use Default Account')] + [(a.id, f"{a.bank_name} - {a.account_number}") for a in bank_accounts]
    
    sender_profiles = SenderProfile.query.filter_by(user_id=current_user.id).all()
    form.sender_profile_id.choices = [(0, 'Use Default Sender Profile')] + [(sp.id, sp.business_name or 'Unnamed Profile') for sp in sender_profiles]
    
    if form.validate_on_submit():
        new_debtor = Debtor(
            user_id=current_user.id,
            name=form.name.data,
            email=form.email.data,
            phone=form.phone.data,
            apply_interest=form.apply_interest.data,
            bank_account_id=form.bank_account_id.data if form.bank_account_id.data != 0 else None,
            sender_profile_id=form.sender_profile_id.data if form.sender_profile_id.data != 0 else None
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
    
    bank_accounts = BusinessBankAccount.query.filter_by(user_id=current_user.id).all()
    form.bank_account_id.choices = [(0, 'Use Default Account')] + [(a.id, f"{a.bank_name} - {a.account_number}") for a in bank_accounts]
    
    sender_profiles = SenderProfile.query.filter_by(user_id=current_user.id).all()
    form.sender_profile_id.choices = [(0, 'Use Default Sender Profile')] + [(sp.id, sp.business_name or 'Unnamed Profile') for sp in sender_profiles]
    
    if request.method == 'GET':
        form.bank_account_id.data = debtor.bank_account_id if debtor.bank_account_id else 0
        form.sender_profile_id.data = debtor.sender_profile_id if debtor.sender_profile_id else 0
        
    if form.validate_on_submit():
        debtor.name = form.name.data
        debtor.email = form.email.data
        debtor.phone = form.phone.data
        debtor.apply_interest = form.apply_interest.data
        debtor.bank_account_id = form.bank_account_id.data if form.bank_account_id.data != 0 else None
        debtor.sender_profile_id = form.sender_profile_id.data if form.sender_profile_id.data != 0 else None
        
        db.session.commit()
        flash("SOA Setup updated successfully.", "success")
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
    all_debtors = Debtor.query.filter_by(user_id=current_user.id).order_by(Debtor.name.asc()).all()
    
    return render_template('program_debtors/debtor_financials.html', debtor=debtor, charges=charges, rc_form=rc_form, ob_form=ob_form, has_opening_balance=has_opening_balance, all_debtors=all_debtors)

@debtors_bp.route("/debtor/<int:debtor_id>/add_charge", methods=["GET", "POST"])
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
            day_of_month=form.day_of_month.data,
            start_date=form.start_date.data,
            end_date=form.end_date.data
        )
        db.session.add(cmap)
        db.session.commit()
        flash("Recurring charge added.", "success")
        return redirect(url_for("debtors_bp.debtor_financials", debtor_id=debtor.id))
        
    my_debtors = Debtor.query.filter_by(user_id=current_user.id).all()
    my_debtor_ids = [d.id for d in my_debtors]
    user_descriptions = db.session.query(DebtorLedger.description)\
        .filter(DebtorLedger.debtor_id.in_(my_debtor_ids))\
        .distinct().all()
    coa_list = [d[0] for d in user_descriptions if d[0]]
        
    return render_template("program_debtors/add_recurring_charge.html", form=form, debtor=debtor, coa_list=coa_list)

@debtors_bp.route("/debtor/<int:debtor_id>/update_interest", methods=["POST"])
@login_required
def update_interest(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    apply_interest = request.form.get('apply_interest') == 'on'
    interest_rate_str = request.form.get('interest_rate')
    
    debtor.apply_interest = apply_interest
    if interest_rate_str and interest_rate_str.strip():
        try:
            debtor.interest_rate = float(interest_rate_str)
        except ValueError:
            flash("Invalid interest rate format.", "danger")
            return redirect(url_for('debtors_bp.dashboard'))
    else:
        debtor.interest_rate = None
        
    db.session.commit()
    flash("Interest settings updated.", "success")
    return redirect(url_for('debtors_bp.dashboard'))

@debtors_bp.route("/debtor/<int:debtor_id>/delete_charge/<int:charge_id>", methods=["POST"])
@login_required
def delete_recurring_charge(debtor_id, charge_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    cmap = DebtorChargeMap.query.filter_by(id=charge_id, debtor_id=debtor.id).first_or_404()
    
    db.session.delete(cmap)
    db.session.commit()
    flash("Recurring charge deleted.", "success")
    return redirect(url_for('debtors_bp.debtor_financials', debtor_id=debtor.id))



@debtors_bp.route("/migrate_charge_dates")
def migrate_charge_dates():
    from sqlalchemy import text
    try:
        db.session.execute(text("ALTER TABLE debtor_charge_map ADD COLUMN start_date DATE"))
    except Exception as e:
        pass
    try:
        db.session.execute(text("ALTER TABLE debtor_charge_map ADD COLUMN end_date DATE"))
    except Exception as e:
        pass
    db.session.commit()
    return "Migration complete."

@debtors_bp.route("/debtor/<int:debtor_id>/add_opening_balance", methods=["GET", "POST"])
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
    return render_template("program_debtors/add_opening_balance.html", form=form, debtor=debtor)

from app.program_debtors.forms import TransactionForm

@debtors_bp.route("/debtor/<int:debtor_id>")
@login_required
def debtor_view(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    start_date = None
    end_date = None
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass

    # Calculate running balance per ledger entry
    running_balance = 0
    all_ledgers = DebtorLedger.query.filter_by(debtor_id=debtor.id).order_by(DebtorLedger.txn_date, DebtorLedger.id).all()
    
    if all_ledgers:
        if not start_date:
            start_date = all_ledgers[0].txn_date
        if not end_date:
            end_date = all_ledgers[-1].txn_date
    
    visible_ledgers = []
    balance_brought_forward = 0
    has_filtered_older = False
    
    for l in all_ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        else:
            running_balance -= l.amount
        l.running_balance = running_balance
        
        include = True
        if start_date and l.txn_date < start_date:
            include = False
            balance_brought_forward = running_balance
            has_filtered_older = True
        if end_date and l.txn_date > end_date:
            include = False
            
        if include:
            visible_ledgers.append(l)
            
    debtor.current_balance = running_balance
    
    return render_template(
        "program_debtors/debtor_view.html", 
        debtor=debtor, 
        ledgers=visible_ledgers, 
        start_date=start_date_str, 
        end_date=end_date_str,
        balance_brought_forward=balance_brought_forward,
        has_filtered_older=has_filtered_older
    )

@debtors_bp.route("/debtor/<int:debtor_id>/add_transaction", methods=["GET", "POST"])
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
        if request.form.get("action") == "save_and_add_another":
            return redirect(url_for("debtors_bp.add_transaction", debtor_id=debtor.id))
        return redirect(url_for("debtors_bp.debtor_view", debtor_id=debtor.id))
    else:
        for field, errors in form.errors.items():
            for error in errors:
                flash(f"Error in {getattr(form, field).label.text}: {error}", "danger")
                
    my_debtors = Debtor.query.filter_by(user_id=current_user.id).all()
    my_debtor_ids = [d.id for d in my_debtors]
    user_descriptions = db.session.query(DebtorLedger.description)\
        .filter(DebtorLedger.debtor_id.in_(my_debtor_ids))\
        .distinct().all()
    coa_list = [d[0] for d in user_descriptions if d[0]]
                
    return render_template("program_debtors/add_transaction.html", form=form, debtor=debtor, coa_list=coa_list)

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
    
    if debtor.sender_profile_id:
        profile = SenderProfile.query.get(debtor.sender_profile_id)
    else:
        profile = SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).first()
        
    if debtor.bank_account_id:
        bank_account = BusinessBankAccount.query.get(debtor.bank_account_id)
    else:
        bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id, is_default=True).first()
    
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
                                   end_date=end_date,
                                   bank_account=bank_account)
    
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
from app.utils.mailer import send_pdf_email

@debtors_bp.route("/email_soa", methods=["POST"])
@login_required
def email_soa():
    from datetime import datetime
    debtor_id = request.form.get("debtor_id")
    to_email = request.form.get("to_email")
    if not debtor_id or not to_email:
        flash("Debtor and email are required.", "danger")
        return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor_id, start_date=request.form.get('start_date'), end_date=request.form.get('end_date')))
        
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    if debtor.sender_profile_id:
        profile = SenderProfile.query.get(debtor.sender_profile_id)
    else:
        profile = SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).first()
        
    if debtor.bank_account_id:
        bank_account = BusinessBankAccount.query.get(debtor.bank_account_id)
    else:
        bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id, is_default=True).first()
        
    start_date_str = request.form.get('start_date')
    end_date_str = request.form.get('end_date')
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    
    # Calculate running balance and fetch ledgers for the entire history
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
                                   bank_account=bank_account,
                                   start_date=start_date,
                                   end_date=end_date)
                                   
    try:
        pdf_bytes = html_to_pdf_bytes(html_content, base_url=request.host_url)
        send_pdf_email(
            to_email=to_email,
            subject=f"Statement of Account - {profile.business_name if profile else 'Billing'}",
            body_text=f"Dear {debtor.name},\n\nPlease find attached your latest Statement of Account.\n\nThank you.",
            pdf_bytes=pdf_bytes,
            filename=f"SOA_{debtor.name.replace(' ', '_')}.pdf"
        )
        flash(f"Statement successfully emailed to {to_email}", "success")
    except Exception as e:
        current_app.logger.error(f"Failed to email SOA PDF: {e}")
        flash(f"Error sending email: {e}", "danger")
        
    return redirect(url_for('debtors_bp.generate_soa', debtor_id=debtor.id, start_date=start_date_str, end_date=end_date_str))




@debtors_bp.route("/debtor/<int:debtor_id>/pay")
@login_required
def pay_soa(debtor_id):
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    # Calculate current balance
    total_debits = sum(l.amount for l in debtor.ledgers if l.kind == 'debit')
    total_credits = sum(l.amount for l in debtor.ledgers if l.kind == 'credit')
    current_balance = total_debits - total_credits
    
    if current_balance <= 0:
        flash("This debtor has no outstanding balance to pay.", "info")
        return redirect(url_for('debtors_bp.dashboard'))
        
    session["pending_email"] = debtor.email or current_user.email
    session["pending_subject"] = "debtors_soa"
    session["zar_amount_cents"] = current_balance
    session["debtor_payment_id"] = debtor.id
    
    return redirect(url_for('paystack_bp.paystack_start'))

@debtors_bp.route("/price")
def price_page():
    from app.models.auth import AuthSubject
    from app.enrollment.logic import get_quote_for_subject_country
    
    subject = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == 'debtors').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code and current_user.is_authenticated:
        from sqlalchemy import text
        ent = db.session.execute(text("""
            SELECT ue.country_code 
              FROM user_enrollment ue
              JOIN auth_subject s ON s.id = ue.subject_id
             WHERE ue.user_id = :uid AND s.slug = 'debtors'
        """), {"uid": current_user.id}).mappings().first()
        if ent and ent["country_code"]:
            country_code = ent["country_code"]

    if not country_code:
        country_code = session.get("country_code", "")

    if country_code:
        session["country_code"] = country_code

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

    if country_code:
        row = get_quote_for_subject_country(subject.id, country_code)
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

    from app.utils.country import get_active_countries
    countries = get_active_countries()

    from app.models.billing import TokenTariff
    tariff = TokenTariff.query.filter_by(program_slug='debtors', action_name='soa_generation').first()
    soa_cents = tariff.base_token_cost * 100 if tariff else 1000

    return render_template("program_debtors/price.html", price=price_ctx, subject=subject, countries=countries, soa_cents=soa_cents)


