from app import create_app
from app.extensions import db

app = create_app()

with app.app_context():
    try:
        from app.models.adv_math import AdvMathProgress
        print("Models imported.")
        
        # Check if table exists
        from sqlalchemy import text
        res = db.session.execute(text("SELECT count(*) FROM adv_math_progress")).scalar()
        print(f"AdvMathProgress count: {res}")
    except Exception as e:
        print(f"Error: {e}")
