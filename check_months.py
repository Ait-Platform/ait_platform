import sys
from app import create_app, db

app = create_app()

with app.app_context():
    rows = db.session.execute(db.text('SELECT DISTINCT month FROM bil_consumption')).fetchall()
    print("Distinct months:", rows)
