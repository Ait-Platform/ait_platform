from app import create_app
from flask import g

app = create_app()
app.config['TESTING'] = True
app.config['WTF_CSRF_ENABLED'] = False

with app.test_client() as client:
    # We need to simulate a logged-in user to avoid redirect to login
    with client.session_transaction() as sess:
        sess['_user_id'] = '1' # assuming user 1 exists
    
    response = client.get('/mechanic/bank_accounts')
    print("STATUS:", response.status_code)
    if response.status_code == 500:
        print(response.data.decode('utf-8'))
