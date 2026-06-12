from app import create_app, db
from app.models.billing import BilTenantLedger

app = create_app()
with app.app_context():
    items = BilTenantLedger.query.order_by(BilTenantLedger.id.desc()).limit(15).all()
    for i in items:
        print(f"{i.id} | {i.tenant_id} | {i.month} | {i.ref} | {i.description} | {i.amount}")
