from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathProgress, AdvMathAssessment
from app.models.auth import User, UserEnrollment

app = create_app()
with app.app_context():
    db.create_all()
    print("Advanced Math tables created successfully.")
