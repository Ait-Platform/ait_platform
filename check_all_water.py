from app import create_app
from app.models.billing import BilConsumption, BilMeter

app = create_app()
with app.app_context():
    meters = BilMeter.query.filter_by(utility_type='water').all()
    for m in meters:
        for c in m.consumptions:
            if c.month == '2026-05':
                print(f"{m.meter_number}: {c.consumption}")
