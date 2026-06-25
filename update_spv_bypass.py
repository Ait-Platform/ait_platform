from wsgi import app
from app.extensions import db
from sqlalchemy import text

with app.app_context():
    subject_id = db.session.execute(text("SELECT id FROM auth_subject WHERE slug='spv'")).scalar()
    
    if subject_id:
        db.session.execute(text("UPDATE auth_subject SET bypass_dashboard_endpoint = 'spv_bp.investor_dashboard' WHERE id = :sid"), {"sid": subject_id})
        db.session.commit()
        print("Successfully updated bypass_dashboard_endpoint for SPV to 'spv_bp.investor_dashboard'")
    else:
        print("SPV subject not found!")
