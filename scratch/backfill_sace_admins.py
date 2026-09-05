from app.extensions import db
from app.models.auth import User, AuthSubject, AuthSubjectAdmin, UserEnrollment
from app import create_app

app = create_app()
with app.app_context():
    # Find all users enrolled in sace_hub
    sace_hub = AuthSubject.query.filter_by(slug='sace_hub').first()
    if not sace_hub:
        print("No sace_hub found")
        exit()
        
    sace_enrollments = UserEnrollment.query.filter_by(subject_id=sace_hub.id).all()
    sace_subjects = AuthSubject.query.filter(AuthSubject.slug.like('sace_%')).all()
    
    count = 0
    for enr in sace_enrollments:
        user = User.query.get(enr.user_id)
        if user:
            # Grant admin for all sace subjects
            for s_subj in sace_subjects:
                existing = AuthSubjectAdmin.query.filter_by(email=user.email, subject_id=s_subj.id).first()
                if not existing:
                    grant = AuthSubjectAdmin(email=user.email, subject_id=s_subj.id)
                    db.session.add(grant)
                    count += 1
    
    db.session.commit()
    print(f"Backfilled {count} admin rights for existing SACE personnel")
