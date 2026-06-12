from app import create_app
from app.models.billing import BilProperty
from app.extensions import db

app = create_app()

with app.app_context():
    props = BilProperty.query.filter_by(name='').all()
    for p in props:
        p.name = f"Unnamed Property (ID: {p.id})"
    db.session.commit()
    print('Renamed blank properties')
