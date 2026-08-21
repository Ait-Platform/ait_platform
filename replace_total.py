import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace job_card.total with calculated total
replacement = '''
    # Calculate total
    labor_total = sum(l.hours * l.rate_per_hour for l in job_card.labor_lines)
    parts_total = sum(p.quantity * p.markup_price for p in job_card.part_lines)
    subtotal = labor_total + parts_total
    vat_amount = subtotal * (job_card.vat_rate / 100.0)
    job_card_total = subtotal + vat_amount

    if not existing_charge and job_card_total > 0:
        charge_ledger = DebtorLedger(
            debtor_id=debtor.id,
            txn_date=datetime.utcnow(),
            kind='debit',
            amount=int(job_card_total * 100),
'''

content = re.sub(
    r"    if not existing_charge and job_card.total > 0:\n\s+charge_ledger = DebtorLedger\(\n\s+debtor_id=debtor.id,\n\s+txn_date=datetime.utcnow\(\),\n\s+kind='debit',\n\s+amount=int\(job_card.total \* 100\),",
    replacement.strip('\n'),
    content
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Replaced successfully")
