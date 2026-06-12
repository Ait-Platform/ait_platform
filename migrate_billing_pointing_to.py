from app.extensions import db
from app import create_app

app = create_app()

with app.app_context():
    try:
        db.session.execute(db.text('ALTER TABLE bil_meter ADD COLUMN IF NOT EXISTS pointing_to VARCHAR(100);'))
        db.session.commit()
        print('Column added successfully!')
    except Exception as e:
        print(f"Error: {e}")
        db.session.rollback()
