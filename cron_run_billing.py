import os
from datetime import datetime
from app import create_app
from app.extensions import db
from app.models.debtors import Debtor, DebtorLedger, DebtorChargeMap

app = create_app()

def run_automated_billing():
    with app.app_context():
        # Get ALL active debtors across all users
        debtors = Debtor.query.filter_by(is_active=True).all()
        current_month_year = datetime.utcnow().strftime("%Y_%m")
        
        charges_applied = 0
        interest_applied = 0
        
        print(f"[{datetime.utcnow()}] Starting automated billing run for {len(debtors)} active debtors...")
        
        for debtor in debtors:
            # Calculate current balance before this month's charges
            total_debits = sum(l.amount for l in debtor.ledgers if l.kind == 'debit')
            total_credits = sum(l.amount for l in debtor.ledgers if l.kind == 'credit')
            current_balance = total_debits - total_credits
    
            # Determine the applicable interest rate
            applicable_rate = debtor.interest_rate if debtor.interest_rate is not None else 0.0
    
            # Apply interest first (if applicable)
            if getattr(debtor, 'apply_interest', True) and current_balance > 0 and applicable_rate > 0:
                interest_ref = f"interest_{current_month_year}"
                existing_interest = DebtorLedger.query.filter_by(debtor_id=debtor.id, ref=interest_ref).first()
                if not existing_interest:
                    interest_amount = int(round(current_balance * (applicable_rate / 100)))
                    if interest_amount > 0:
                        ledger = DebtorLedger(
                            debtor_id=debtor.id,
                            description=f"Monthly Arrears Interest ({applicable_rate}%)",
                            kind="debit",
                            amount=interest_amount,
                            ref=interest_ref,
                            txn_date=datetime.utcnow()
                        )
                        db.session.add(ledger)
                        interest_applied += 1
    
            today = datetime.utcnow().date()
            for cmap in debtor.charge_maps:
                if cmap.start_date and today < cmap.start_date:
                    continue
                if cmap.end_date and today > cmap.end_date:
                    continue
                    
                ref_id = f"recurring_{cmap.id}_{current_month_year}"
                
                # Check if this charge was already applied this month
                existing_ledger = DebtorLedger.query.filter_by(debtor_id=debtor.id, ref=ref_id).first()
                if not existing_ledger:
                    # Apply the charge
                    ledger = DebtorLedger(
                        debtor_id=debtor.id,
                        description=cmap.charge_description,
                        kind="debit",
                        amount=cmap.amount,
                        ref=ref_id,
                        txn_date=datetime.utcnow()
                    )
                    db.session.add(ledger)
                    charges_applied += 1
                    
        if charges_applied > 0 or interest_applied > 0:
            db.session.commit()
            print(f"[{datetime.utcnow()}] SUCCESS: Applied {charges_applied} recurring charge(s) and {interest_applied} interest charge(s).")
        else:
            print(f"[{datetime.utcnow()}] FINISHED: All recurring charges for the current month have already been applied.")

if __name__ == "__main__":
    run_automated_billing()
