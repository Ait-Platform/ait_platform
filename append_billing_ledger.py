from datetime import datetime

with open('app/program_billing/routes.py', 'a', encoding='utf-8') as f:
    f.write('''

# ==========================================
# BILLING WALLED GARDEN LEDGER (DEBTORS)
# ==========================================

@billing_bp.route("/manager/tenant_accounts")
@login_required
def tenant_accounts():
    from app.models.debtors import Debtor
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')
    
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    except ValueError:
        start_date = None
        end_date = None

    try:
        # Get all tenants (managed by Debtors table with slug_reference='billing')
        debtors = Debtor.query.filter_by(user_id=current_user.id, slug_reference='billing').all()
        total_owed = 0
        
        for d in debtors:
            valid_ledgers = d.ledgers
            if start_date:
                valid_ledgers = [l for l in valid_ledgers if l.txn_date >= start_date]
            if end_date:
                valid_ledgers = [l for l in valid_ledgers if l.txn_date <= end_date]
                
            total_debits = sum(l.amount for l in valid_ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in valid_ledgers if l.kind == 'credit')
            d.current_balance = (total_debits - total_credits) / 100.0
            
            if d.current_balance > 0:
                total_owed += d.current_balance
                
    except Exception as e:
        current_app.logger.error(f"Error loading tenant accounts: {e}")
        debtors = []
        total_owed = 0
        
    return render_template("program_billing/tenant_accounts.html", 
                           debtors=debtors, 
                           total_owed=total_owed,
                           start_date=start_date_str,
                           end_date=end_date_str)


@billing_bp.route("/manager/tenant_ledger/<int:debtor_id>")
@login_required
def tenant_ledger(debtor_id):
    from app.models.debtors import Debtor, BusinessBankAccount
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id, slug_reference='billing').first_or_404()
    
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')
    
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    except ValueError:
        start_date = None
        end_date = None

    # Filter ledgers based on date
    filtered_ledgers = debtor.ledgers
    if start_date:
        filtered_ledgers = [l for l in filtered_ledgers if l.txn_date >= start_date]
    if end_date:
        filtered_ledgers = [l for l in filtered_ledgers if l.txn_date <= end_date]

    filtered_ledgers.sort(key=lambda x: x.txn_date)

    running_balance = 0
    for l in filtered_ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        elif l.kind == 'credit':
            running_balance -= l.amount
        l.running_balance = running_balance

    # For statement generation shortcut
    bank_account = BusinessBankAccount.query.filter_by(user_id=current_user.id, is_default=True).first()

    return render_template("program_billing/tenant_ledger.html", 
                           debtor=debtor, 
                           ledgers=reversed(filtered_ledgers),
                           bank_account=bank_account,
                           start_date=start_date_str,
                           end_date=end_date_str)


@billing_bp.route("/manager/tenant_ledger/<int:debtor_id>/add_transaction", methods=["POST"])
@login_required
def tenant_ledger_add_transaction(debtor_id):
    from app.models.debtors import Debtor, DebtorLedger
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id, slug_reference='billing').first_or_404()

    try:
        txn_date_str = request.form.get("txn_date")
        txn_date = datetime.strptime(txn_date_str, "%Y-%m-%d").date() if txn_date_str else datetime.utcnow().date()
        kind = request.form.get("kind", "credit")  # default to payment/credit
        amount = int(float(request.form.get("amount", 0)) * 100)
        description = request.form.get("description", "Thank you for this payment")
        ref = request.form.get("ref", "")

        if amount > 0:
            ledger_entry = DebtorLedger(
                debtor_id=debtor.id,
                description=description,
                ref=ref,
                kind=kind,
                amount=amount,
                txn_date=txn_date
            )
            db.session.add(ledger_entry)
            db.session.commit()
            flash("Transaction successfully recorded.", "success")
        else:
            flash("Amount must be greater than 0.", "error")
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Failed to add tenant transaction: {e}")
        flash("Failed to record transaction. Invalid input.", "error")

    return redirect(url_for('billing_bp.tenant_ledger', debtor_id=debtor.id))


@billing_bp.route("/manager/bank_accounts")
@login_required
def bank_accounts():
    from app.models.debtors import BusinessBankAccount
    accounts = BusinessBankAccount.query.filter_by(user_id=current_user.id).all()
    return render_template("program_billing/bank_accounts.html", accounts=accounts)
''')
