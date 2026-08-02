from app import create_app
from app.extensions import db
from app.models.auth import User, UserEnrollment, AuthSubject
from app.models.practice_crm import CrmPractice, CrmPracticeUser, CrmEnquiry

DATABASE_URL = "postgresql://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

app = create_app()
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL

with app.app_context():
    try:
        user = User.query.filter_by(email='san@gmail.com').first()
        if not user:
            print("User not found!")
        else:
            print("Found user:", user.email, user.id)
            practice = CrmPractice.query.filter_by(owner_id=user.id).first()
            if not practice:
                print("No practice found. Creating one...")
                practice = CrmPractice(owner_id=user.id, name=f"{user.name or 'My'} Practice")
                db.session.add(practice)
                db.session.commit()
                print("Created practice:", practice.id)
            else:
                print("Found practice:", practice.id, practice.name)
                
            enquiries = CrmEnquiry.query.filter_by(practice_id=practice.id).all()
            print("Found enquiries:", len(enquiries))
            
    except Exception as e:
        import traceback
        traceback.print_exc()
