
from app import create_app, db
from app.models.culturalfire import CfiPageantQuestion
app = create_app()
with app.app_context():
    qs = CfiPageantQuestion.query.all()
    print(f'Found {len(qs)} questions.')
    for q in qs[:5]:
        print(q.id, q.question_text)

