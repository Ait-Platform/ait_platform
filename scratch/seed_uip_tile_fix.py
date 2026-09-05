import sys, os
from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='uip').first()
    if not subj:
        subj = AuthSubject(
            slug='uip',
            name='UIP Platform',
            program_type='B2B',
            show_on_welcome=True,
            about_endpoint='uip_bp.uip_start'
        )
        db.session.add(subj)
    else:
        subj.show_on_welcome = True
        subj.about_endpoint = 'uip_bp.uip_start'
        
    db.session.commit()
    print("UIP AuthSubject seeded!")
