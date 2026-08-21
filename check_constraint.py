from app import create_app, db
from sqlalchemy import text
app = create_app()
with app.app_context():
    print(db.session.execute(text("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck_auth_subject_processor_default';")).scalar())
