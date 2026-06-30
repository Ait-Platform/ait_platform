import os
from io import BytesIO
from app import create_app
from app.extensions import db
from flask_login import login_user
from app.models.auth import User

app = create_app()
app.config['WTF_CSRF_ENABLED'] = False
app.config['TESTING'] = True

with app.test_client() as client:
    with app.app_context():
        # Login as first user
        user = User.query.first()
        if user:
            with client.session_transaction() as sess:
                sess['_user_id'] = str(user.id)
                sess['_fresh'] = True
                
    data = {
        'bill_file': (BytesIO(b'dummy pdf content'), 'test.pdf')
    }
    
    response = client.post('/api/parse_bill_onboarding', data=data, content_type='multipart/form-data')
    print(f"Status: {response.status_code}")
    print(f"Response: {response.data.decode('utf-8')}")
