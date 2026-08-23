import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''    try:
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
                debtors.append(d)'''

content = re.sub(
    r"    try:\s*debtors = Debtor\.query\.filter_by\(user_id=current_user\.id, slug_reference='mechanic'\)\.all\(\)\s*total_owed = 0\s*for d in debtors:\s*valid_ledgers = d\.ledgers\s*if start_date:\s*valid_ledgers = \[l for l in valid_ledgers if l\.txn_date >= start_date\]\s*if end_date:\s*valid_ledgers = \[l for l in valid_ledgers if l\.txn_date <= end_date\]\s*total_debits = sum\(l\.amount for l in valid_ledgers if l\.kind == 'debit'\)\s*total_credits = sum\(l\.amount for l in valid_ledgers if l\.kind == 'credit'\)\s*d\.current_balance = \(total_debits - total_credits\) / 100\.0\s*if d\.current_balance > 0:\s*total_owed \+= d\.current_balance",
    replacement,
    content,
    flags=re.DOTALL
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
