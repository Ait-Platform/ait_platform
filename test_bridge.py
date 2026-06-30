import os
from app import create_app
from flask_login import login_user
from app.models.auth import User

app = create_app()
app.config['WTF_CSRF_ENABLED'] = False
app.config['TESTING'] = True

with app.test_client() as client:
    with app.app_context():
        user = User.query.first()
        if user:
            with client.session_transaction() as sess:
                sess['_user_id'] = str(user.id)
                sess['_fresh'] = True
                
    response = client.get('/dashboard')
    print(f"Dashboard Status: {response.status_code}")
    if response.status_code != 200:
        print(response.data.decode('utf-8')[:200])
