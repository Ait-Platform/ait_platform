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

@mechanic_bp.route("/mechanic/debtors_schedule")
@login_required
def debtors_schedule():
    from app.models.debtors import Debtor
    from datetime import datetime
    
    start_date_str = request.args.get('start_date', '')
    end_date_str = request.args.get('end_date', '')
    
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None
    except ValueError:
        start_date = None
        end_date = None

    try:
        all_debtors = Debtor.query.filter_by(user_id=current_user.id, slug_reference='mechanic').all()
        total_owed = 0
        debtors = []
        
        for d in all_debtors:
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
                debtors.append(d)
                
    except Exception as e:
        debtors = []
        total_owed = 0
        
    return render_template("program_mechanic/debtors_schedule.html", 
                           debtors=debtors, 
                           total_owed=total_owed,
                           start_date=start_date_str,
                           end_date=end_date_str)'''

content = re.sub(
    r"@mechanic_bp\.route\(\"/mechanic/client_accounts\"\)\s*@login_required\s*def client_accounts\(\).*?return render_template\(\"program_mechanic/client_accounts\.html\",\s*debtors=debtors,\s*total_owed=total_owed,\s*start_date=start_date_str,\s*end_date=end_date_str\)",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
