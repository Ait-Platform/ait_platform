# app/cli/debtors_jobs.py
from datetime import datetime
from sqlalchemy import text
from flask import current_app
from app.extensions import db
from app.models.debtors import Debtor, DebtorLedger, SoaProfile, DebtorChargeMap

def run_debtors_billing_job():
    """
    Runs the monthly billing for all active debtors in the system.
    This calculates interest and applies recurring charges.
    """
    current_app.logger.info("Starting automated Debtors billing sweep...")
    
    debtors = Debtor.query.filter_by(is_active=True).all()
    count = 0
    
    for debtor in debtors:
        profile = SoaProfile.query.filter_by(user_id=debtor.user_id).first()
        if not profile:
            continue
            
        now = datetime.utcnow()
        current_year = now.year
        current_month = now.month
        month_str = f"{current_year}_{current_month:02d}"
        
        # 1) Arrears Interest Calculation
        if debtor.apply_interest and profile.interest_rate > 0:
            already_charged_interest = DebtorLedger.query.filter_by(
                debtor_id=debtor.id, 
                ref=f"interest_{month_str}"
            ).first()
            
            if not already_charged_interest:
                total_debits = sum(l.amount for l in debtor.ledgers if l.kind == 'debit')
                total_credits = sum(l.amount for l in debtor.ledgers if l.kind == 'credit')
                current_balance = total_debits - total_credits
                
                if current_balance > 0:
                    interest_amount = int(round(current_balance * (profile.interest_rate / 100.0)))
                    if interest_amount > 0:
                        interest_ledger = DebtorLedger(
                            debtor_id=debtor.id,
                            description=f"Monthly Arrears Interest ({profile.interest_rate}%)",
                            kind="debit",
                            amount=interest_amount,
                            ref=f"interest_{month_str}",
                            txn_date=now
                        )
                        db.session.add(interest_ledger)
                        
        # 2) Recurring Charges
        recurring = DebtorChargeMap.query.filter_by(debtor_id=debtor.id).all()
        for r in recurring:
            already_charged = DebtorLedger.query.filter_by(
                debtor_id=debtor.id,
                ref=f"rec_{r.id}_{month_str}"
            ).first()
            
            if not already_charged:
                new_ledger = DebtorLedger(
                    debtor_id=debtor.id,
                    description=r.charge_description,
                    kind="debit",
                    amount=r.amount,
                    ref=f"rec_{r.id}_{month_str}",
                    txn_date=now
                )
                db.session.add(new_ledger)
                
        count += 1
        
    db.session.commit()
    current_app.logger.info(f"Automated Debtors billing complete. Processed {count} debtors.")

