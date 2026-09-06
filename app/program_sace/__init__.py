from flask import Blueprint

sace_bp = Blueprint("sace_bp", __name__, template_folder="../../templates")

def auto_patch_sace(app):
    with app.app_context():
        from app.extensions import db
        from sqlalchemy import text
        queries = [
            # 1) Endorsement Track (Free, no token wallet needed, manual codes)
            """
            INSERT INTO auth_subject (slug, name, is_active, sort_order, program_type, commercial_mode, requires_price)
            SELECT 'sace_endorsement', 'SACE Provider Endorsement', 1, 10, 'free', 'free', 0
            WHERE NOT EXISTS (SELECT 1 FROM auth_subject WHERE slug = 'sace_endorsement');
            """,
            # 2) Live Teacher Track (Paid, public access)
            """
            INSERT INTO auth_subject (slug, name, is_active, sort_order, program_type, commercial_mode, requires_price)
            SELECT 'sace_teacher', 'SACE Live Teacher Training', 1, 11, 'single', 'paid', 1
            WHERE NOT EXISTS (SELECT 1 FROM auth_subject WHERE slug = 'sace_teacher');
            """
        ]
        for q in queries:
            db.session.execute(text(q))
        db.session.commit()

sace_bp.record(lambda state: auto_patch_sace(state.app))

from . import routes
