from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathQuestion

app = create_app()
with app.app_context():
    # Check if we already have it
    existing = AdvMathQuestion.query.filter_by(topic_name='algebra', sub_topic='sequences_geometric').first()
    if not existing:
        q1 = AdvMathQuestion(
            topic_name='algebra', 
            sub_topic='sequences_geometric', 
            question_text='Consider the geometric sequence: 2, 6, 18, 54, ... Determine the nth term and calculate the sum of the first 10 terms.', 
            correct_answer='T_n = 2(3)^(n-1) and S_10 = 59048', 
            explanation='The common ratio is 3. Use the geometric sum formula.', 
            question_type='open', 
            source_paper='2022 NSC Math Paper 1'
        )
        db.session.add(q1)
        db.session.commit()
        print('Mock question inserted successfully!')
    else:
        print('Question already exists.')
