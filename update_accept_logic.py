import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

accept_logic = '''def accept_quote(id):
    from app.models.debtors import Debtor, SenderProfile, DebtorLedger
    from datetime import datetime
    
    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status == 'Quote':
        job_card.status = 'Awaiting Deposit'
        
        # Ensure Debtor profile exists
        client = job_card.vehicle.client
        debtor = None
        if client:
            debtor = Debtor.query.filter_by(user_id=current_user.id, slug_reference='mechanic', name=client.name).first()
            if not debtor:
                sender_profile = SenderProfile.query.filter_by(user_id=current_user.id, is_default=True).first()
                debtor = Debtor(
                    user_id=current_user.id,
                    name=client.name,
                    phone=client.phone,
                    email=client.email,
                    slug_reference='mechanic',
                    sender_profile_id=sender_profile.id if sender_profile else None
                )
                db.session.add(debtor)
                db.session.flush() # get id
                
            # Log the full quote amount as a Debit
            existing_charge = DebtorLedger.query.filter_by(
                debtor_id=debtor.id, 
                ref=f"JOB-{job_card.job_number}", 
                kind='debit'
            ).first()
            
            if not existing_charge:
                labor_total = sum(l.hours * l.rate_per_hour for l in job_card.labor_lines)
                parts_total = sum(p.quantity * p.markup_price for p in job_card.part_lines)
                subtotal = labor_total + parts_total
                vat_amount = subtotal * (job_card.vat_rate / 100.0)
                job_card_total = subtotal + vat_amount
                
                if job_card_total > 0:
                    charge_ledger = DebtorLedger(
                        debtor_id=debtor.id,
                        txn_date=db.func.current_date(),
                        kind='debit',
                        amount=int(job_card_total * 100),
                        description=f"Quote/Tax Invoice for Job #{job_card.job_number}",
                        ref=f"JOB-{job_card.job_number}"
                    )
                    db.session.add(charge_ledger)
        
        db.session.commit()
        flash("Quote accepted! Invoice posted to ledger.", "success")
        
        if debtor:
            return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))
            
    return redirect(url_for('mechanic_bp.job_card_detail', id=id))'''

content = re.sub(
    r"def accept_quote\(id\):.*?return redirect\(url_for\('mechanic_bp\.job_card_detail', id=id\)\)",
    accept_logic,
    content,
    flags=re.DOTALL
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
