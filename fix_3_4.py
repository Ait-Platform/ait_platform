from app import create_app
from app.extensions import db
from app.models.home import HomeQuestionOption, HomeQuestion
app = create_app()
app.app_context().push()
q18=HomeQuestion.query.get(18)
q19=HomeQuestion.query.get(19)
q19.question = 'Which bucket holds more water?'
q18.correct_answer, q19.correct_answer = q19.correct_answer, q18.correct_answer
opts18 = HomeQuestionOption.query.filter_by(question_id=18).all()
opts19 = HomeQuestionOption.query.filter_by(question_id=19).all()
for o in opts18: o.question_id = 19
for o in opts19: o.question_id = 18
db.session.commit()
print('Fixed Q18 and Q19')
