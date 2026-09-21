from app import create_app
from app.extensions import db
from app.models.uip import UipResolutionVote, UipResolution
from app.models.auth import User

app = create_app()
with app.app_context():
    votes = UipResolutionVote.query.all()
    print(f"Total votes in DB: {len(votes)}")
    for v in votes:
        u = User.query.get(v.user_id)
        r = UipResolution.query.get(v.resolution_id)
        print(f"Res ID {v.resolution_id} ('{r.title if r else 'None'}') - User ID {v.user_id} ('{u.name if u else 'None'}') - Vote: {v.vote}")
