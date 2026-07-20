import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import User
from app.program_culturalfire.routes import cultural_fire_router

app = create_app()

with app.app_context():
    user = User.query.filter_by(email='minor@test.com').first()
    if not user:
        user = User(name='Minor Test', email='minor@test.com')
        db.session.add(user)
        db.session.commit()
    
    # simulate login
    from flask_login import login_user
    
    # We need a request context to test routing
    with app.test_request_context('/program/cultural_fire'):
        login_user(user)
        try:
            cultural_fire_router()
            print("cultural_fire_router ran successfully without 500 error!")
        except Exception as e:
            import traceback
            print("ERROR IN ROUTER:")
            traceback.print_exc()
