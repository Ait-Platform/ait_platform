import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    query = db.session.query(DebtorLedger).filter_by(debtor_id=debtor.id)
    if start_date:
        query = query.filter(DebtorLedger.txn_date >= start_date)
    if end_date:
        query = query.filter(DebtorLedger.txn_date <= end_date)
        
    transactions = query.order_by(DebtorLedger.txn_date.asc(), DebtorLedger.id.asc()).all()

    # Calculate running balances
    running_balance = 0
    for txn in transactions:
        if txn.kind == 'debit':
            running_balance += txn.amount
        elif txn.kind == 'credit':
            running_balance -= txn.amount
        txn.running_balance = running_balance
        
    # Fetch job cards for this client
    from app.models.mechanic import MechJobCard, MechVehicle, MechClient
    job_cards = MechJobCard.query.join(MechVehicle).join(MechClient).filter(MechClient.name == debtor.name, MechClient.user_id == current_user.id).order_by(MechJobCard.created_at.desc()).all()

    return render_template('program_mechanic/client_ledger.html', 
                           debtor=debtor, 
                           transactions=transactions,
                           start_date=start_date_str,
                           end_date=end_date_str,
                           job_cards=job_cards)'''

content = re.sub(
    r"    query = db\.session\.query\(DebtorLedger\)\.filter_by\(debtor_id=debtor\.id\).*?return render_template\('program_mechanic/client_ledger\.html',\s*debtor=debtor,\s*transactions=transactions,\s*start_date=start_date_str,\s*end_date=end_date_str\)",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
