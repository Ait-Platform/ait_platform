import traceback
from app import create_app, db
from app.models.auth import User
from flask_login import login_user
from app.program_culturalfire.routes import showcase_dashboard
from flask import current_app

app = create_app()
with app.test_request_context('/showcase/dashboard'):
    try:
        user = User.query.first()
        if user:
            login_user(user)
        res = showcase_dashboard()
        print("SUCCESS")
    except Exception as e:
        print("ERROR THROWN:")
        traceback.print_exc()
