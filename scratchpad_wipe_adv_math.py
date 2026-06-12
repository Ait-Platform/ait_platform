import os
from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathQuestion

app = create_app()
with app.app_context():
    count = AdvMathQuestion.query.delete()
    db.session.commit()
    print(f"Deleted {count} questions from the database.")
