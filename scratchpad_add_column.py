from app.extensions import db
from sqlalchemy import text

def add_enrollment_id_column():
    try:
        db.session.execute(text("ALTER TABLE cfi_judge_assignment ADD COLUMN enrollment_id INTEGER REFERENCES user_enrollment(id)"))
        db.session.commit()
        print("Successfully added enrollment_id to cfi_judge_assignment.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    from app import create_app
    app = create_app()
    with app.app_context():
        add_enrollment_id_column()
