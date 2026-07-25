import traceback
from app import create_app, db
from app.models.auth import User
from flask_login import login_user
from app.program_culturalfire.routes import judge_dashboard
from flask import current_app

app = create_app()
with app.test_request_context('/judge/dashboard?origin=talent&enrollment_id=1'):
    try:
        user = User.query.first()
        if user:
            login_user(user)
        res = judge_dashboard()
        print("SUCCESS")
    except Exception as e:
        print("ERROR THROWN:")
        traceback.print_exc()
