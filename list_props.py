from app import create_app
from app.extensions import db
from app.models.billing import BilProperty

app = create_app()
with app.app_context():
    props = BilProperty.query.all()
    for p in props:
        print(f"Prop ID: {p.id}, Name: {p.name}")
