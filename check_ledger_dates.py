from app import create_app, db
from app.models.billing import BilTenantLedger

app = create_app()
with app.app_context():
    items = BilTenantLedger.query.order_by(BilTenantLedger.id.desc()).limit(3).all()
    for i in items:
        print(f"{i.id} | {i.ref} | {i.txn_date}")

    # Let's also delete them so they get recreated in the right order with proper IDs next time
    for i in items:
        db.session.delete(i)
    db.session.commit()
