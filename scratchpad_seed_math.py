import os
from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathQuestion

app = create_app()
with app.app_context():
    # Insert some dummy questions for testing the new UI/grading flow
    q1 = AdvMathQuestion(
        topic_name="algebra",
        source_paper="Nov 2023 Paper 1",
        question_type="long_form",
        question_text="Solve for x: x^2 - 5x + 6 = 0",
        correct_answer="x = 2 or x = 3",
        explanation="Factorize the quadratic equation: (x-2)(x-3) = 0."
    )
    
    q2 = AdvMathQuestion(
        topic_name="calculus",
        source_paper="Nov 2023 Paper 1",
        question_type="mcq",
        question_text="What is the derivative of f(x) = x^3?",
        option_a="3x^2",
        option_b="x^2",
        option_c="3x",
        option_d="x^4/4",
        correct_answer="3x^2",
        explanation="Use the power rule: bring down the exponent and subtract 1 from the power."
    )
    
    db.session.add(q1)
    db.session.add(q2)
    db.session.commit()
    print("Seeded mock questions.")
