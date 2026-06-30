import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

injection = '''
class MechShop(db.Model):
    __tablename__ = 'mech_shops'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    business_name = db.Column(db.String(150))
    address = db.Column(db.Text)
    phone = db.Column(db.String(50))
    email = db.Column(db.String(150))
    registration_number = db.Column(db.String(100))
    tax_number = db.Column(db.String(100))
    logo_url = db.Column(db.String(255))
    terms_and_conditions = db.Column(db.Text)
    onboarding_status = db.Column(db.String(50), default='draft_setup') # draft_setup, draft_review, active
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class MechCatalogPart(db.Model):
    __tablename__ = 'mech_catalog_parts'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True) # Null for global pre-populated, specific for learned
    part_name = db.Column(db.String(150), nullable=False)
    category = db.Column(db.String(50)) # e.g. Engine, Brakes, Suspension
    default_price = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
'''

content = content.replace("class MechClient(db.Model):", injection + "\nclass MechClient(db.Model):")

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)
