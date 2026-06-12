from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE adv_math_question ADD COLUMN sub_topic VARCHAR(100);"))
        db.session.commit()
        print("Added sub_topic to adv_math_question.")
    except Exception as e:
        db.session.rollback()
        print("sub_topic error:", e)

    try:
        db.session.execute(text("ALTER TABLE adv_math_progress ADD COLUMN mastery_data JSON;"))
        db.session.commit()
        print("Added mastery_data to adv_math_progress.")
    except Exception as e:
        db.session.rollback()
        print("mastery_data error:", e)
