from app import create_app
from app.models.billing import BilConsumption

app = create_app()
with app.app_context():
    c = BilConsumption.query.filter_by(meter_number='AGN489').first()
    print(c.month if c else 'Not found')
