import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

elec_old = '''def build_electrical_rows(tenant_id, month):
    rows = []
    total_due = 0

    linked_meter_ids = [m.id for m in BilMeter.query.filter_by(sectional_unit_id=tenant_id).all()]'''

elec_new = '''def build_electrical_rows(property_id, month):
    from app.models.billing import BilSectionalUnit
    rows = []
    total_due = 0

    unit_ids = [u.id for u in BilSectionalUnit.query.filter_by(property_id=property_id).all()]
    linked_meter_ids = [m.id for m in BilMeter.query.filter(BilMeter.sectional_unit_id.in_(unit_ids)).all()]'''

text = text.replace(elec_old, elec_new)

water_old = '''def build_water_rows(tenant_id, month):
    water_meters = []
    total_water_due = 0

    linked_meter_ids = [m.id for m in BilMeter.query.filter_by(sectional_unit_id=tenant_id).all()]'''

water_new = '''def build_water_rows(property_id, month):
    from app.models.billing import BilSectionalUnit
    water_meters = []
    total_water_due = 0

    unit_ids = [u.id for u in BilSectionalUnit.query.filter_by(property_id=property_id).all()]
    linked_meter_ids = [m.id for m in BilMeter.query.filter(BilMeter.sectional_unit_id.in_(unit_ids)).all()]'''

text = text.replace(water_old, water_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated build_electrical_rows and build_water_rows to use property_id')
