import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''@mechanic_bp.route("/mechanic/client_accounts")
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
        debtors = []
        
    return render_template("program_mechanic/client_accounts.html", debtors=debtors)

@mechanic_bp.route("/mechanic/generate_debtors_schedule", methods=["POST"])
@login_required
def generate_debtors_schedule():
    from app.models.debtors import Debtor
    from app.models.mechanic import MechShop
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    from app.models.billing import TokenTariff
    from datetime import datetime
    
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    
    tariff = TokenTariff.query.filter_by(program_slug='mechanic', action_name='generate_schedule').first()
    if not tariff:
        tariff = TokenTariff(program_slug='mechanic', action_name='generate_schedule', base_token_cost=10)
        db.session.add(tariff)
        db.session.commit()
        
    token_cost = tariff.base_token_cost
    
    if not wallet or wallet.balance < token_cost:
        flash(f"You need {token_cost} tokens to generate a Debtors Schedule.", "error")
        return redirect(url_for("payment_bp.wallet_topup", subject_slug="mechanic"))
        
    # Deduct tokens
    wallet.balance -= token_cost
    txn = AitTokenTransaction(
        wallet_id=wallet.id,
        amount=-token_cost,
        description="Generated Debtors Schedule (Balance Sheet)"
    )
    db.session.add(txn)
    db.session.commit()
    
    start_date_str = request.form.get('start_date', '')
    end_date_str = request.form.get('end_date', '')
    
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    except ValueError:
        start_date = None
        end_date = None

    shop = MechShop.query.filter_by(user_id=current_user.id).first()

    try:
        debtors = Debtor.query.filter_by(user_id=current_user.id, slug_reference='mechanic').all()
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
        debtors = []
        total_owed = 0
        
    return render_template("program_mechanic/debtors_schedule.html", 
                           debtors=debtors, 
                           total_owed=total_owed,
                           start_date=start_date_str,
                           end_date=end_date_str,
                           shop=shop)'''

content = re.sub(
    r"@mechanic_bp\.route\(\"/mechanic/client_accounts\"\).*?def client_accounts\(\).*?end_date=end_date_str,\s*shop=shop\)",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
