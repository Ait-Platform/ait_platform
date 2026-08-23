import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''@mechanic_bp.route("/mechanic/client_ledger/<int:debtor_id>/add_payment", methods=["POST"])
@login_required
def client_ledger_add_payment(debtor_id):
    from app.models.debtors import Debtor, DebtorLedger
    from datetime import datetime
    
    debtor = Debtor.query.filter_by(id=debtor_id, user_id=current_user.id).first_or_404()
    
    try:
        amount_str = request.form.get('amount', '0')
        amount = int(float(amount_str) * 100)
        ref = request.form.get('ref', '')
        desc = request.form.get('description', 'Payment')
        kind = request.form.get('kind', 'credit')
        
        if kind not in ['credit', 'debit']:
            kind = 'credit'
            
        date_str = request.form.get('date')
        txn_date = datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else db.func.current_date()
        
        if amount > 0:
            ledger = DebtorLedger(
                debtor_id=debtor.id,
                txn_date=txn_date,
                ref=ref,
                description=desc,
                kind=kind,
                amount=amount
            )'''

content = re.sub(
    r"@mechanic_bp\.route\(\"/mechanic/client_ledger/<int:debtor_id>/add_payment\", methods=\[\"POST\"\]\)\s*@login_required\s*def client_ledger_add_payment\(debtor_id\):\s*from app\.models\.debtors import Debtor, DebtorLedger\s*from datetime import datetime\s*debtor = Debtor\.query\.filter_by\(id=debtor_id, user_id=current_user\.id\)\.first_or_404\(\)\s*try:\s*amount_str = request\.form\.get\('amount', '0'\)\s*amount = int\(float\(amount_str\) \* 100\)\s*ref = request\.form\.get\('ref', ''\)\s*desc = request\.form\.get\('description', 'Manual Payment'\)\s*date_str = request\.form\.get\('date'\)\s*txn_date = datetime\.strptime\(date_str, '%Y-%m-%d'\)\.date\(\) if date_str else db\.func\.current_date\(\)\s*if amount > 0:\s*ledger = DebtorLedger\(\s*debtor_id=debtor\.id,\s*txn_date=txn_date,\s*ref=ref,\s*description=desc,\s*kind='credit',\s*amount=amount\s*\)",
    replacement,
    content
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
