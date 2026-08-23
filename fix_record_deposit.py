import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''            # Log deposit as a credit transaction in DebtorLedger
            ledger = DebtorLedger(
                debtor_id=debtor.id,
                date=db.func.current_date(),
                ref=f"DEP-{job_card.job_number}",
                description=f"Deposit for Job #{job_card.job_number}",
                amount=deposit_amount,
                kind="credit"
            )''',
    '''            # Log deposit as a credit transaction in DebtorLedger
            ledger = DebtorLedger(
                debtor_id=debtor.id,
                txn_date=db.func.current_date(),
                ref=f"DEP-{job_card.job_number}",
                description=f"Deposit for Job #{job_card.job_number}",
                amount=int(deposit_amount * 100),
                kind="credit"
            )'''
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
