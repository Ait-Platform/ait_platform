from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN assigned_to INTEGER REFERENCES \"user\"(id);"))
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN closed_by INTEGER REFERENCES \"user\"(id);"))
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN channel VARCHAR(50);"))
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN category VARCHAR(100);"))
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN closed_at TIMESTAMP WITHOUT TIME ZONE;"))
        db.session.commit()
        print("Interaction Columns added successfully.")
    except Exception as e:
        print(f"Error adding columns: {e}")
        db.session.rollback()
