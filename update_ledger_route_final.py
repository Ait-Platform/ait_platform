import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    for l in ledgers:
        if l.kind == 'debit':
            running_balance += l.amount
        else:
            running_balance -= l.amount
        l.running_balance = running_balance

    # Fetch job cards for this client
    from app.models.mechanic import MechJobCard, MechVehicle, MechClient
    job_cards = MechJobCard.query.join(MechVehicle).join(MechClient).filter(MechClient.name == debtor.name, MechClient.user_id == current_user.id).order_by(MechJobCard.created_at.desc()).all()

    return render_template("program_mechanic/client_ledger.html", 
                           debtor=debtor, 
                           ledgers=ledgers, 
                           job_cards=job_cards,
                           start_date=start_date_str, 
                           end_date=end_date_str)'''

content = re.sub(
    r"    for l in ledgers:\s*if l\.kind == 'debit':\s*running_balance \+= l\.amount\s*else:\s*running_balance -= l\.amount\s*l\.running_balance = running_balance\s*return render_template\(\"program_mechanic/client_ledger\.html\",\s*debtor=debtor,\s*ledgers=ledgers,\s*start_date=start_date_str,\s*end_date=end_date_str\)",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
