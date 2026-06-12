from app import create_app
from app.extensions import db

app = create_app()
app.testing = True
c = app.test_client()

with c.session_transaction() as sess:
    sess['email'] = 'san@gmail.com'
    sess['user_id'] = 1

res = c.get('/start_registration?subject=home2', follow_redirects=True)
print("Status:", res.status_code)
print("Final URL:", res.request.url)
print("Flashes:", [msg for category, msg in res.get_flashed_messages(with_categories=True)])
