from app import create_app
from app.extensions import db
from app.models.mechanic import MechShop, MechCatalogPart

app = create_app()
with app.app_context():
    MechShop.__table__.create(db.engine, checkfirst=True)
    MechCatalogPart.__table__.create(db.engine, checkfirst=True)
    print("Tables created successfully.")
