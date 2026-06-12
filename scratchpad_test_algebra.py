import os
from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathQuestion

app = create_app()
with app.app_context():
    count = AdvMathQuestion.query.filter_by(topic_name='algebra').count()
    print(f"ALGEBRA QUESTIONS: {count}")
    
    # Also print all topics for good measure
    from sqlalchemy import func
    topics = db.session.query(AdvMathQuestion.topic_name, func.count(AdvMathQuestion.id)).group_by(AdvMathQuestion.topic_name).all()
    for t in topics:
        print(f"{t[0]}: {t[1]}")
