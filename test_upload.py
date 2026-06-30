import json
from app import create_app
from app.models.auth import User
from io import BytesIO

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
                
    # Create a dummy image file
    data = {
        'bill_file': (BytesIO(b'dummy image content'), 'test.jpg')
    }
    response = client.post('/api/parse_bill_onboarding', data=data, content_type='multipart/form-data')
    print(f'Status: {response.status_code}')
    print(response.data.decode('utf-8'))
