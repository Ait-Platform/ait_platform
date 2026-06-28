from app import create_app
from app.extensions import db
from app.models.auth import ApprovedAdmin, UserEnrollment, User, AuthSubject
from app.subject_loss.services import compute_loss_results
from sqlalchemy import text
app = create_app()
with app.app_context():
    # Fix the missing scores bug for all loss runs
    runs = db.session.execute(text("SELECT id FROM lca_run WHERE status IN ('completed', 'finished')")).fetchall()
    for (rid,) in runs:
        compute_loss_results(rid)
    print('Recalculated scores for runs.')

    # Fix the loss@gmail.com bridge problem
    admin = ApprovedAdmin.query.filter_by(email='loss@gmail.com').first()
    if admin:
        print('loss@gmail.com is an admin! Removing...')
        db.session.delete(admin)
        db.session.commit()
    else:
        print('loss@gmail.com is NOT an admin.')
        
    user = User.query.filter_by(email='loss@gmail.com').first()
    if user:
        for en in UserEnrollment.query.filter_by(user_id=user.id).all():
            subj = AuthSubject.query.get(en.subject_id)
            if subj and subj.slug != 'loss':
                print(f'Removing extra enrollment: {subj.slug}')
                db.session.delete(en)
        db.session.commit()
