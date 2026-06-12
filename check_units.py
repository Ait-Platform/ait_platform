from app import create_app
from app.models.billing import BilMeter, BilSectionalUnit

app = create_app()
with app.app_context():
    units = BilSectionalUnit.query.filter_by(property_id=8).all()
    for u in units:
        print(f"Unit {u.id} meters: {[m.meter_number for m in u.meters]}")
