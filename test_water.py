from app import create_app
from app.models.billing import BilProperty, BilSectionalUnit
from app.school_billing.routes import build_water_rows
import json

app = create_app()
with app.app_context():
    p = BilProperty.query.get(8)
    unit = BilSectionalUnit.query.filter_by(property_id=p.id).first()
    water_meters, water_total = build_water_rows(unit.id, '2026-05')
    
    # Just print the first water meter to see its structure
    w = water_meters[0]
    print(json.dumps(w, indent=2, default=str))
    print("GRAND TOTAL:", water_total)
