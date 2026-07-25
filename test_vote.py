import traceback
from app import create_app, db
from app.models.culturalfire import CfiShowcaseVote, CfiJudgeScore
from app.models.auth import User
from flask_login import login_user
from app.program_culturalfire.routes import vote_item
from flask import request

app = create_app()
with app.test_request_context('/show/vote', json={
    'submission_id': 1,
    'type': 'talent',
    'score': 50,
    'crit1': 10, 'crit2': 10, 'crit3': 10, 'crit4': 10, 'crit5': 10
}):
    try:
        user = User.query.first()
        login_user(user)
        res = vote_item()
        print("RES:", res.get_data(as_text=True))
    except Exception as e:
        traceback.print_exc()
