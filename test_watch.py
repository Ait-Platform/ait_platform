import sys
from app import create_app, db
from app.models.auth import User
app = create_app()
app.testing = True
with app.test_client() as c:
    with app.app_context():
        user = User.query.first()
    with c.session_transaction() as sess:
        sess['_user_id'] = str(user.id)
    res = c.get('/cultural/show/watch/1')
    print(res.status_code)
