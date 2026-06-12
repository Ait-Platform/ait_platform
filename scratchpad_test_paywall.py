import os
from app import create_app
from app.extensions import db
from app.models.auth import User
from flask import url_for

app = create_app()

with app.test_client() as client:
    with app.app_context():
        # log in as testnew123@gmail.com
        user = User.query.filter_by(email="testnew123@gmail.com").first()
        print(f"User: {user.email}")
        
    # login using test client
    with client.session_transaction() as sess:
        sess['_user_id'] = str(user.id)
        sess['_fresh'] = True
        
    # hit chapter 11
    response = client.get('/home/chapter/11')
    print(f"Status Code: {response.status_code}")
    print(f"Location Header: {response.headers.get('Location')}")
