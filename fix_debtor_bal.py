import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_bal = '''            total_debits = sum(l.amount for l in d.ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in d.ledgers if l.kind == 'credit')
            bal = total_debits - total_credits
            if bal > 0:
                d.current_balance = bal
                debtors_with_balances.append(d)'''

new_bal = '''            total_debits = sum(l.amount for l in d.ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in d.ledgers if l.kind == 'credit')
            bal = total_debits - total_credits
            if bal > 0:
                d.current_balance = bal / 100.0
                debtors_with_balances.append(d)'''

content = content.replace(old_bal, new_bal)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
