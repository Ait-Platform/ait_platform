from app import create_app
from app.models.billing import BilMeter, BilSectionalUnit

app = create_app()
with app.app_context():
    m = BilMeter.query.filter_by(meter_number='AGN489').first()
    if m:
        print("Meter found, id:", m.id)
        if m.sectional_unit_id:
            u = BilSectionalUnit.query.get(m.sectional_unit_id)
            print("Sectional Unit:", u.id, "Property:", u.property_id)
        else:
            print("Meter not linked to any sectional unit!")
    else:
        print("Meter not found!")
