from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    # Check if sace_cptd exists
    existing = AuthSubject.query.filter_by(slug='sace_cptd').first()
    if not existing:
        new_subj = AuthSubject(
            slug='sace_cptd',
            name='Sace CPTD Reading Activity',
            is_active=1,
            program_type='course'
        )
        db.session.add(new_subj)
        db.session.commit()
        print("Added SACE CPTD to database")
    else:
        print("SACE CPTD already exists")
