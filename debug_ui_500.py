from app import create_app
from app.extensions import db
from app.models.auth import User
import traceback

app = create_app()

with app.app_context():
    # Login as User 1 (or any user that exists)
    user = User.query.first()
    
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['_user_id'] = str(user.id)
            sess['_fresh'] = True
            
        print(f"Testing as user: {user.email}")
        
        print("\n--- Testing /show/program/15 ---")
        response = client.get('/show/program/15')
        print(f"Status Code: {response.status_code}")
        if response.status_code == 500:
            print(response.data.decode()[:1000]) # Print first 1000 chars of 500 error page
            
        print("\n--- Testing /show/watch/15 ---")
        response = client.get('/show/watch/15')
        print(f"Status Code: {response.status_code}")
        if response.status_code == 500:
            print(response.data.decode()[:1000])
