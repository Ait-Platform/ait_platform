from wsgi import app
from app.extensions import db
from app.models.billing import BilArchitectureDraft
import json

with app.app_context():
    draft = BilArchitectureDraft.query.filter_by(property_id=32).first()
    if draft:
        data = draft.draft_json
        print(f"subWater: {data.get('subWater', [])}")
        print(f"subElec: {data.get('subElec', [])}")
        print(f"mapping: {data.get('mapping', [])}")
        print(f"initialReadings: {data.get('initialReadings', [])}")
