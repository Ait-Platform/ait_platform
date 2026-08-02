from app import create_app
from app.extensions import db
from app.models.auth import User, UserEnrollment, AuthSubject
import sys

app = create_app()
app.config['TESTING'] = True
app.config['WTF_CSRF_ENABLED'] = False

with app.test_client() as client:
    with app.app_context():
        # Get or create a test user
        user = User.query.filter_by(email='san@gmail.com').first()
        if not user:
            user = User(email='san@gmail.com', name='San')
            db.session.add(user)
            db.session.commit()
            
        # Ensure they have practice_crm enrollment
        subj = AuthSubject.query.filter_by(slug='practice_crm').first()
        if subj:
            enr = UserEnrollment.query.filter_by(user_id=user.id, subject_id=subj.id).first()
            if not enr:
                enr = UserEnrollment(user_id=user.id, subject_id=subj.id, status='active')
                db.session.add(enr)
                db.session.commit()
        user_id_str = str(user.id)
    
    # Login
    with client.session_transaction() as sess:
        sess['email'] = 'san@gmail.com'
        # Flask-Login requires _user_id
        sess['_user_id'] = user_id_str
        sess['_fresh'] = True

    # Hit the route
    response = client.get('/practice-crm/pipeline')
    print("STATUS:", response.status_code)
    if response.status_code >= 400:
        print("ERROR DATA:", response.data.decode('utf-8'))
