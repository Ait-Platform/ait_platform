import sys
from app import create_app, db

app = create_app()

with app.app_context():
    rows = db.session.execute(db.text('SELECT meter_id, month, consumption FROM bil_consumption')).mappings().all()
    print([dict(r) for r in rows])
