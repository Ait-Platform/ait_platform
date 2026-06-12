from app.extensions import db
from app import create_app

app = create_app()

with app.app_context():
    try:
        db.session.execute(db.text('ALTER TABLE bil_sectional_unit ADD COLUMN IF NOT EXISTS property_id INTEGER REFERENCES bil_property(id);'))
        db.session.commit()
        print('Column added successfully!')
    except Exception as e:
        print(f"Error: {e}")
        db.session.rollback()
