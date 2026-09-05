from app import create_app
from app.extensions import db

app = create_app()
with app.app_context():
    res = db.session.execute(db.text("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck_auth_subject_processor_default'")).scalar()
    print("CONSTRAINT:", res)
