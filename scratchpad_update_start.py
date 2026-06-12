from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='adv_math').first()
    if subj:
        subj.start_endpoint = 'adv_math_bp.about'
        db.session.commit()
        print(f"Updated start_endpoint for {subj.slug} to adv_math_bp.about")
    else:
        print("Subject adv_math not found")
