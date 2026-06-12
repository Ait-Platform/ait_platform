from app import create_app
from app.school_billing.helpers import get_dashboard_data
from flask_login import login_user
from app.models.auth import User

app = create_app()
with app.app_context():
    with app.test_request_context():
        user = User.query.first()
        login_user(user)
        print("Data:", get_dashboard_data())
