from app import create_app
from app.extensions import db
from app.models.billing import BilProperty, BilSectionalUnit

app = create_app()
with app.app_context():
    p = BilProperty.query.get(8)
    units = BilSectionalUnit.query.filter_by(property_id=p.id).all()
    for u in units:
        print(f"Unit {u.id} - Tenants: {[t.id for t in u.tenants]}")
