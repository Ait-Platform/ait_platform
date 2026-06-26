from app import create_app  
from app.extensions import db  
from sqlalchemy import text  
app = create_app()  
with app.app_context():  
    db.session.execute(text(\" UPDATE auth_subject SET program_type=paid commercial_mode=paid start_endpoint=loss_bp.subject_home WHERE "slug=loss\))  
    db.session.commit()  
    print(\Database" endpoints for loss updated to paid and start_endpoint "fixed.\) 
