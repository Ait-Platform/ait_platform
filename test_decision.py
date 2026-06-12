from app import create_app
from app.extensions import db
from app.models.auth import User

app = create_app()
app.testing = True

with app.test_client() as c:
    with c.session_transaction() as sess:
        sess['user_id'] = 1
        sess['email'] = 'test@example.com'
    
    # We must patch Flask-Login or just let the test client login properly
    # The easiest way is to hit the real /login endpoint first if we can,
    # but let's just make the user.is_authenticated True if we hit the endpoint.
    
    # Actually, we can just hit /register/decision?subject=home2 directly!
    res = c.get('/register/decision?subject=home2', follow_redirects=True)
    print("Status:", res.status_code)
    print("Final URL:", res.request.url)
    
    # Print the flashed messages from the session
    with c.session_transaction() as sess:
        print("Session:", dict(sess))
