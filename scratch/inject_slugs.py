from app import create_app
from app.extensions import db
from sqlalchemy import text
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    # Insert new SACE slugs
    queries = [
        # 1) Endorsement Track (Free, no token wallet needed, manual codes)
        """
        INSERT INTO auth_subject (slug, name, description, is_active, sort_order, program_type, commercial_mode, requires_price)
        SELECT 'sace_endorsement', 'SACE Provider Endorsement', 'VIP Auditor Track for SACE Endorsement', 1, 10, 'free', 'free', 0
        WHERE NOT EXISTS (SELECT 1 FROM auth_subject WHERE slug = 'sace_endorsement');
        """,
        # 2) Live Teacher Track (Paid, public access)
        """
        INSERT INTO auth_subject (slug, name, description, is_active, sort_order, program_type, commercial_mode, requires_price)
        SELECT 'sace_teacher', 'SACE Live Teacher Training', 'Public Paid Track for SA Teachers', 1, 11, 'single', 'paid', 1
        WHERE NOT EXISTS (SELECT 1 FROM auth_subject WHERE slug = 'sace_teacher');
        """
    ]
    for q in queries:
        db.session.execute(text(q))
    db.session.commit()
    print("DB slugs updated successfully.")
