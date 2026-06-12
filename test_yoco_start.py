from app import create_app
from app.extensions import db
from flask import url_for

app = create_app()
app.testing = True

with app.test_client() as c:
    # 1. Login
    # Using the /dev-login/reading/admin endpoint or similar might work,
    # or just mocking the session:
    with c.session_transaction() as sess:
        sess['user_id'] = 1
        sess['email'] = 'test@example.com'
    
    # Actually, we can just hit /yoco/start directly!
    res = c.get('/yoco/start?subject=home2&email=test@example.com', follow_redirects=False)
    print("Yoco Start Status:", res.status_code)
    print("Redirect Location:", res.headers.get('Location'))
    
    with c.session_transaction() as sess:
        flashes = sess.get('_flashes', [])
        print("Flashes:", flashes)
