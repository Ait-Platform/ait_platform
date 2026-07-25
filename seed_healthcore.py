from app import create_app, db
from app.models.auth import AuthSubject, User, UserEnrollment

app = create_app()

with app.app_context():
    # 1. Create the HealthCore Subject
    s = AuthSubject.query.filter_by(slug='healthcore').first()
    if not s:
        s = AuthSubject(
            slug='healthcore', 
            name='HealthCore', 
            description='The next generation of personalized health dashboards.'
        )
        db.session.add(s)
        db.session.commit()
        print('Added HealthCore subject!')
    
    # 2. Enroll users so it appears on their Bridge Dashboard
    users = User.query.all()
    count = 0
    for u in users:
        enr = UserEnrollment.query.filter_by(user_id=u.id, subject_id=s.id).first()
        if not enr:
            enr = UserEnrollment(user_id=u.id, subject_id=s.id, status='active')
            db.session.add(enr)
            count += 1
            
    db.session.commit()
    print(f'Enrolled {count} users in HealthCore.')