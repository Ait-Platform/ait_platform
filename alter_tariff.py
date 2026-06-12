import sqlite3
from app import create_app, db

app = create_app()
with app.app_context():
    try:
        db.session.execute(db.text('ALTER TABLE bil_tariff ADD COLUMN reduction_factor FLOAT'))
        db.session.commit()
        print("Added reduction_factor")
    except Exception as e:
        db.session.rollback()
        print("reduction_factor already exists or error:", e)

    try:
        db.session.execute(db.text('ALTER TABLE bil_tariff ADD COLUMN unit VARCHAR(20)'))
        db.session.commit()
        print("Added unit")
    except Exception as e:
        db.session.rollback()
        print("unit already exists or error:", e)
