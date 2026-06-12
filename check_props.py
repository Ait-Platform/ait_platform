from app import create_app
from app.models.billing import BilProperty
from app.extensions import db

app = create_app()

with app.app_context():
    props = BilProperty.query.all()
    print("Properties:")
    for p in props:
        print(f"ID: {p.id}, Name: {p.name}")
