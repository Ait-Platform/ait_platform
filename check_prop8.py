from app import create_app
from app.extensions import db
from app.models.billing import BilProperty, BilSectionalUnit

app = create_app()
with app.app_context():
    p = BilProperty.query.get(8)
    if p:
        print(f"Prop: {p.name}")
        for u in p.units:
            print(f"  Unit {u.id}: {u.unit_number}")
            for t in u.tenants:
                print(f"    Tenant {t.id}: {t.name}")
    else:
        print("Prop 8 not found")
