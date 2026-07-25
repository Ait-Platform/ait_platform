import traceback
from app import create_app, db
from app.models.auth import User
from flask_login import login_user
from app.program_culturalfire.routes import showcase_dashboard

app = create_app()
with app.test_request_context('/showcase/dashboard'):
    try:
        user = User.query.first()
        if user:
            login_user(user)
        # Render the template to catch template errors!
        res = showcase_dashboard()
        if isinstance(res, str):
            print("RENDERED OK")
        else:
            print("RESPONSE:", res)
    except Exception as e:
        print("ERROR THROWN:")
        traceback.print_exc()
