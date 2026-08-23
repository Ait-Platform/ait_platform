import os

routes_path = 'app/program_mechanic/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    content = f.read()

new_routes = '''

@mechanic_bp.route("/mechanic/client_accounts")
@login_required
def client_accounts():
    from app.models.debtors import Debtor
    try:
        debtors = Debtor.query.filter_by(user_id=current_user.id, slug_reference='mechanic').all()
        for d in debtors:
            total_debits = sum(l.amount for l in d.ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in d.ledgers if l.kind == 'credit')
            d.current_balance = (total_debits - total_credits) / 100.0
    except Exception as e:
        current_app.logger.error(f"Error loading client accounts: {e}")
        debtors = []
        
    return render_template("program_mechanic/client_accounts.html", debtors=debtors)


@mechanic_bp.route("/mechanic/client_ledger/<int:debtor_id>")
@login_required
def client_ledger(debtor_id):
    from app.models.debtors import Debtor
    from datetime import datetime
    
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

    ledgers_query = debtor.ledgers
    if start_date:
        ledgers_query = [l for l in ledgers_query if l.txn_date and l.txn_date >= start_date]
    if end_date:
        ledgers_query = [l for l in ledgers_query if l.txn_date and l.txn_date <= end_date]

    ledgers = sorted(ledgers_query, key=lambda l: (l.txn_date or datetime.min.date(), l.id))
    
    running_balance = 0
    for l in ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        else:
            running_balance -= l.amount
        l.running_balance = running_balance

    return render_template("program_mechanic/client_ledger.html", 
                           debtor=debtor, 
                           ledgers=ledgers, 
                           start_date=start_date_str, 
                           end_date=end_date_str)


@mechanic_bp.route("/mechanic/client_ledger/<int:debtor_id>/add_payment", methods=["POST"])
@login_required
def client_ledger_add_payment(debtor_id):
    from app.models.debtors import Debtor, DebtorLedger
    from datetime import datetime
    
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    try:
        amount_str = request.form.get('amount', '0')
        amount = int(float(amount_str) * 100)
        ref = request.form.get('ref', '')
        desc = request.form.get('description', 'Manual Payment')
        
        date_str = request.form.get('date')
        txn_date = datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else db.func.current_date()
        
        if amount > 0:
            ledger = DebtorLedger(
                debtor_id=debtor.id,
                txn_date=txn_date,
                ref=ref,
                description=desc,
                kind='credit',
                amount=amount
            )
            db.session.add(ledger)
            db.session.commit()
            flash(f"Payment of R {amount/100.0:.2f} recorded successfully.", "success")
        else:
            flash("Amount must be greater than 0.", "warning")
            
    except Exception as e:
        db.session.rollback()
        flash(f"Error recording payment: {e}", "danger")
        
    return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))
'''

content = content + new_routes

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(content)
