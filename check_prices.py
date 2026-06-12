import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    for sid in [1, 2, 4]:
        count = db.session.execute(text(f"SELECT COUNT(*) FROM subject_country_price WHERE subject_id = {sid}")).scalar()
        print(f"Subject {sid} has {count} prices.")
