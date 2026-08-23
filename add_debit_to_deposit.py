import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

debit_logic = '''
            # Step 1: Charge the full quote amount as a Debit (if not already charged)
            existing_charge = DebtorLedger.query.filter_by(
                debtor_id=debtor.id, 
                ref=f"JOB-{job_card.job_number}", 
                kind='debit'
            ).first()
            
            # Calculate total
            labor_total = sum(l.hours * l.rate_per_hour for l in job_card.labor_lines)
            parts_total = sum(p.quantity * p.markup_price for p in job_card.part_lines)
            subtotal = labor_total + parts_total
            vat_amount = subtotal * (job_card.vat_rate / 100.0)
            job_card_total = subtotal + vat_amount
        
            if not existing_charge and job_card_total > 0:
                charge_ledger = DebtorLedger(
                    debtor_id=debtor.id,
                    txn_date=db.func.current_date(),
                    kind='debit',
                    amount=int(job_card_total * 100),
                    description=f"Quote/Tax Invoice for Job #{job_card.job_number}",
                    ref=f"JOB-{job_card.job_number}"
                )
                db.session.add(charge_ledger)
            
            # Step 2: Log payment as a credit transaction in DebtorLedger
'''

content = content.replace(
    '''            # Log deposit as a credit transaction in DebtorLedger''',
    debit_logic
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
