from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    cptd = AuthSubject.query.filter_by(slug='cptd').first()
    if cptd:
        cptd.start_endpoint = 'cptd_bp.hub'
        
    thunee = AuthSubject.query.filter_by(slug='thunee').first()
    if thunee:
        thunee.start_endpoint = 'thunee_bp.index'

    db.session.commit()
    print("Updated endpoints successfully")
