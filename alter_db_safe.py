from app import create_app, db
from sqlalchemy import text
import traceback

app = create_app()
with app.app_context():
    try:
        # Check if column exists first
        result = db.session.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='core_interaction' AND column_name='parent_id';")).fetchall()
        if not result:
            db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN parent_id INTEGER REFERENCES core_interaction(id);"))
            db.session.commit()
            print("Successfully added parent_id to core_interaction")
        else:
            print("parent_id already exists")
    except Exception as e:
        db.session.rollback()
        print("Failed to alter table:")
        traceback.print_exc()
