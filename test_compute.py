from app import create_app
from app.extensions import db
from app.subject_loss.services import compute_loss_results
from sqlalchemy import text

app = create_app()
with app.app_context():
    print("Before compute:")
    print(db.session.execute(text("SELECT * FROM lca_result WHERE run_id=36")).fetchall())
    
    compute_loss_results(36)
    
    print("After compute:")
    print(db.session.execute(text("SELECT * FROM lca_result WHERE run_id=36")).fetchall())
