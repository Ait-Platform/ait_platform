from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE adv_math_question ADD COLUMN concepts_tested TEXT"))
        db.session.commit()
        print("Successfully added concepts_tested column to adv_math_question")
    except Exception as e:
        print(f"Error adding column: {e}")
