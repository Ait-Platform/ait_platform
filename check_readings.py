from app import create_app
from app.extensions import db
from app.models.billing import BilProperty, BilConsumption, BilSectionalUnit

app = create_app()
with app.app_context():
    p = BilProperty.query.get(8)
    unit = BilSectionalUnit.query.filter_by(property_id=p.id).first()
    print('Unit meters:', [(m.id, m.meter_number) for m in unit.meters] if unit else 'None')
    cons = BilConsumption.query.filter_by(month='2026-05').all()
    print('All May Cons:', [(c.meter_number, c.consumption) for c in cons])
