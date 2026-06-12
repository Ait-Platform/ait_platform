from app import create_app
from app.extensions import db
from app.models.billing import BilProperty, BilSectionalUnit
from app.school_billing.routes import build_electrical_rows

app = create_app()
with app.app_context():
    p = BilProperty.query.get(8)
    unit = BilSectionalUnit.query.filter_by(property_id=p.id).first()
    rows, total = build_electrical_rows(unit.id, '2026-05')
    print("Num rows:", len(rows))
    print("Total:", total)
    for r in rows:
        print(r)
