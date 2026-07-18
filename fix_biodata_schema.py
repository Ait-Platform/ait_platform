from app import create_app, db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text("ALTER TABLE cfi_biodata ALTER COLUMN id_number DROP NOT NULL;"))
    db.session.execute(text("ALTER TABLE cfi_biodata ALTER COLUMN phone DROP NOT NULL;"))
    db.session.commit()
    print("Successfully dropped NOT NULL constraints on id_number and phone.")
