import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''@mechanic_bp.route("/mechanic/client_accounts")
@login_required
def client_accounts():
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
        current_app.logger.error(f"Error loading client accounts: {e}")
        debtors = []
        total_owed = 0
        
    return render_template("program_mechanic/client_accounts.html", 
                           debtors=debtors, 
                           total_owed=total_owed,
                           start_date=start_date_str,
                           end_date=end_date_str)'''

content = re.sub(
    r"@mechanic_bp\.route\(\"/mechanic/client_accounts\"\)\s*@login_required\s*def client_accounts\(\):\s*from app\.models\.debtors import Debtor\s*try:\s*debtors = Debtor\.query\.filter_by\(user_id=current_user\.id, slug_reference='mechanic'\)\.all\(\)\s*for d in debtors:\s*total_debits = sum\(l\.amount for l in d\.ledgers if l\.kind == 'debit'\)\s*total_credits = sum\(l\.amount for l in d\.ledgers if l\.kind == 'credit'\)\s*d\.current_balance = \(total_debits - total_credits\) / 100\.0\s*except Exception as e:\s*current_app\.logger\.error\(f\"Error loading client accounts: \{e\}\"\)\s*debtors = \[\]\s*return render_template\(\"program_mechanic/client_accounts\.html\", debtors=debtors\)",
    replacement,
    content
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
