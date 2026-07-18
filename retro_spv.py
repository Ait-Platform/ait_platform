from app import create_app, db
app = create_app()
with app.app_context():
    from app.models.spv import SpvParticipation
    from sqlalchemy import text
    spv_id = db.session.execute(text("SELECT id FROM auth_subject WHERE slug = 'spv'")).scalar()
    parts = SpvParticipation.query.all()
    for p in parts:
        uid = p.user_id
        existing = db.session.execute(text("SELECT id FROM user_enrollment WHERE user_id = :uid AND subject_id = :sid"), {'uid': uid, 'sid': spv_id}).first()
        if not existing:
            db.session.execute(text("INSERT INTO user_enrollment (user_id, subject_id, status) VALUES (:uid, :sid, 'active')"), {'uid': uid, 'sid': spv_id})
            db.session.commit()
            print(f'Granted spv enrollment to user {uid}')
