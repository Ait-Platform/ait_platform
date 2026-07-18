from app import create_app
from app.extensions import db
from app.models.culturalfire import CfiShow
app = create_app()
with app.app_context():
    shows = CfiShow.query.all()
    for s in shows:
        print(s.id, s.title, s.description)
