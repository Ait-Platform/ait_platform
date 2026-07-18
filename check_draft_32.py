from wsgi import app
from app.extensions import db
from app.models.billing import BilArchitectureDraft
import json

with app.app_context():
    draft = BilArchitectureDraft.query.filter_by(property_id=32).first()
    if draft:
        data = draft.draft_json
        print(f"Meters: {len(data.get('meters', []))}")
        print(f"Accounts: {len(data.get('accounts', []))}")
        print(f"Rates: {len(data.get('rates', []))}")
        print(f"Arrears: {len(data.get('arrears', []))}")
        print(f"Initial Readings: {len(data.get('initialReadings', []))}")
    else:
        print("No draft found for Dale")
