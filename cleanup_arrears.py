from app import create_app, db
from app.models.billing import BilTenantLedger, BilTenantRecurring

app = create_app()
with app.app_context():
    # 1. Delete the bad ledger entry
    bad_ledger = BilTenantLedger.query.filter(BilTenantLedger.ref.like('%AUTO:ARREARS%')).all()
    for b in bad_ledger:
        db.session.delete(b)
        print(f"Deleted bad ledger entry: {b.id} - {b.description}")

    # 2. Delete or deactivate the recurring item
    bad_recurs = BilTenantRecurring.query.filter(BilTenantRecurring.description.like('%Arrears%')).all()
    for r in bad_recurs:
        db.session.delete(r)
        print(f"Deleted bad recurring item: {r.id} - {r.description}")

    # 3. Clean up any old "Arrears" in the ledger for May 2026 if it didn't get overwritten
    # Wait, the script output showed:
    # 4 | 3 | 2026-05 | ARREARS-AUTO | Opening Balance | 100000.00
    # So the one in May is perfectly fine and is called "Opening Balance".

    db.session.commit()
    print("Cleanup complete.")
