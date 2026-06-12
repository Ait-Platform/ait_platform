from app import create_app
from app.models.billing import BilMeter

app = create_app()

with app.app_context():
    meters = BilMeter.query.all()
    for m in meters:
        print(f"ID: {m.id}, Number: {m.meter_number}, Parent: {m.parent_meter_id}, Unit: {m.sectional_unit_id}")
