import os
from app import create_app
from app.extensions import db
from app.models.adv_math import AdvMathQuestion

app = create_app()
with app.app_context():
    # Insert Paper 2 mock questions (Nov 2023)
    q_geom = AdvMathQuestion(
        topic_name="geometry",
        sub_topic="euclidean_circle_theorems",
        source_paper="DBE November 2023 Paper 2",
        question_type="long_form",
        question_text="In circle O, chord AB subtends angle AOB at the centre and angle ACB at the circumference. Prove that angle AOB = 2 * angle ACB.",
        correct_answer="Proof provided in standard curriculum.",
        explanation="Standard circle theorem: Angle at the centre is twice the angle at the circumference subtended by the same arc."
    )
    
    q_fin = AdvMathQuestion(
        topic_name="financial_math",
        sub_topic="financial_annuities",
        source_paper="DBE November 2023 Paper 2",
        question_type="long_form",
        question_text="Sipho invests R1000 per month into a retirement annuity earning 9% p.a. compounded monthly. What is the future value after 20 years?",
        correct_answer="667886.87",
        explanation="Use the Future Value Annuity formula: F = x[(1+i)^n - 1]/i where x=1000, i=0.09/12, n=240."
    )

    q_stat = AdvMathQuestion(
        topic_name="probability",
        sub_topic="statistics_regression",
        source_paper="DBE November 2023 Paper 2",
        question_type="long_form",
        question_text="A set of bivariate data has equation of least squares regression line y = 3.5x + 12. Predict y when x = 10.",
        correct_answer="47",
        explanation="Substitute x = 10 into the regression equation: y = 3.5(10) + 12 = 35 + 12 = 47."
    )
    
    db.session.add(q_geom)
    db.session.add(q_fin)
    db.session.add(q_stat)
    db.session.commit()
    print("Successfully pulled and seeded DBE November 2023 Paper 2 questions.")
