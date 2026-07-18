from app import create_app, db
from sqlalchemy import text
app = create_app()
with app.app_context():
    rows = db.session.execute(text("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'subject_country_price'")).fetchall()
    for r in rows:
        print(f'{r.column_name}: nullable={r.is_nullable}')
