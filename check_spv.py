from wsgi import app
from app.extensions import db
from sqlalchemy import text

with app.app_context():
    # Let's inspect auth_subject
    subjects = db.session.execute(text("SELECT id, slug, name, is_active, is_hidden_on_bridge, program_type FROM auth_subject")).fetchall()
    print(f"Total subjects: {len(subjects)}")
    for s in subjects:
        print(f"ID={s.id}, slug={s.slug}, name={s.name}, is_active={s.is_active}, is_hidden={s.is_hidden_on_bridge}, type={s.program_type}")
    
    # Let's see enrollments for spv@gmail.com
    user_id = db.session.execute(text("SELECT id FROM \"user\" WHERE email='spv@gmail.com'")).scalar()
    if user_id:
        enrolls = db.session.execute(text("SELECT subject_id, status FROM user_enrollment WHERE user_id=:uid"), {"uid": user_id}).fetchall()
        print(f"Enrollments for spv@gmail.com:")
        for e in enrolls:
            print(f"Subject_id={e.subject_id}, Status={e.status}")
