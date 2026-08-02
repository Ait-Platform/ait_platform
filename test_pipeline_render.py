from app import create_app
from app.extensions import db
from app.models.auth import User, UserEnrollment, AuthSubject
import sys

DATABASE_URL = "postgresql://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

app = create_app()
app.config['TESTING'] = True
app.config['WTF_CSRF_ENABLED'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL

with app.test_client() as client:
    with app.app_context():
        user = User.query.filter_by(email='san@gmail.com').first()
        subj = AuthSubject.query.filter_by(slug='practice_crm').first()
        user_id_str = str(user.id)
    
    # Login
    with client.session_transaction() as sess:
        sess['email'] = 'san@gmail.com'
        sess['_user_id'] = user_id_str
        sess['_fresh'] = True

    # Hit the route
    response = client.get('/practice-crm/pipeline')
    print("STATUS:", response.status_code)
    if response.status_code >= 400:
        print("ERROR DATA:")
        print(response.data.decode('utf-8'))
