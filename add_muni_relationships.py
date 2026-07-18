import re

with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_fields = """    water_meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='SET NULL'))
    elec_meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='SET NULL'))"""

new_fields = """    water_meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='SET NULL'))
    elec_meter_id = db.Column(db.Integer, db.ForeignKey('bil_meter.id', ondelete='SET NULL'))
    
    water_meter = db.relationship('BilMeter', foreign_keys=[water_meter_id])
    elec_meter = db.relationship('BilMeter', foreign_keys=[elec_meter_id])
    owner = db.relationship('RefMuniOwner', foreign_keys=[owner_id])"""

if old_fields in content:
    content = content.replace(old_fields, new_fields)
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added relationships to BilMuniAccount")
else:
    print("Could not find fields to replace.")
