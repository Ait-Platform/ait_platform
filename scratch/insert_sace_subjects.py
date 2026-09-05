from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    subjects = [
        ('sace_evaluator', 'SACE Evaluation Portal'),
        ('sace_facilitator', 'SACE Workshop Facilitator'),
        ('sace_participant', 'SACE CPTD Reading Activity')
    ]
    
    for slug, name in subjects:
        # Check if exists
        exists = db.session.execute(text("SELECT id FROM auth_subject WHERE slug = :s"), {"s": slug}).scalar()
        if not exists:
            db.session.execute(text("""
                INSERT INTO auth_subject (
                    slug, name, is_active, program_type, commercial_mode, enroll_policy, processor_default
                ) VALUES (
                    :slug, :name, 1, 'course', 'free', 'auto_enroll', 'yoco'
                )
            """), {"slug": slug, "name": name})
            print(f"Inserted: {slug}")
        else:
            print(f"Already exists: {slug}")
            
    db.session.commit()
    print("Database updated.")
