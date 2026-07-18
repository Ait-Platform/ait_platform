from app import create_app, db
from app.models.culturalfire import CfiShowcaseVote
app = create_app()
with app.app_context():
    print(CfiShowcaseVote.__tablename__)
