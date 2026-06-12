from app import create_app
from app.extensions import db
from app.models.home import HomeQuestionOption, HomeQuestion
app = create_app()
app.app_context().push()
q19=HomeQuestion.query.get(19)
q20=HomeQuestion.query.get(20)
q19.correct_answer, q20.correct_answer = q20.correct_answer, q19.correct_answer
opts19 = HomeQuestionOption.query.filter_by(question_id=19).all()
opts20 = HomeQuestionOption.query.filter_by(question_id=20).all()
for o in opts19:
    o.question_id=20
for o in opts20:
    o.question_id=19
db.session.commit()
print('Swapped options successfully')
