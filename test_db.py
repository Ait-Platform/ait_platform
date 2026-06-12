from app import create_app
from app.models.billing import BilSectionalUnit, BilProperty, BilTenant, BilMeter
from app.extensions import db

app = create_app()

with app.app_context():
    print(BilSectionalUnit.__table__.columns.keys())
