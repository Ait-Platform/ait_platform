from app import create_app, db
from sqlalchemy import text
app = create_app()
with app.app_context():
    rows = db.session.execute(text("SELECT COUNT(*) FROM subject_country_price WHERE subject_id = 12")).scalar()
    print(f'Rows in subject_country_price for CFI: {rows}')
