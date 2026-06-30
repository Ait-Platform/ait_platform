with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_model = '''
class BilPlatformSettings(db.Model):
    __tablename__ = 'bil_platform_settings'
    id = db.Column(db.Integer, primary_key=True)
    base_price_cents = db.Column(db.Integer, default=10000)
    included_meters = db.Column(db.Integer, default=2)
    extra_meter_price_cents = db.Column(db.Integer, default=1500)
'''

text += new_model

with open('app/models/billing.py', 'w', encoding='utf-8') as f:
    f.write(text)

from app.extensions import db
from app import create_app
app = create_app()
with app.app_context():
    db.create_all()

print('Updated billing models and created table.')
