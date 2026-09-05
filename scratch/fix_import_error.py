import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Replace the bad import and ORM query
bad_block = '''    # Also check reading module progress
    from app.models.reading import RDPEnrollment
    reading_enr = RDPEnrollment.query.filter_by(user_id=current_user.id).first()
    reading_completed = reading_enr is not None and reading_enr.progress_percent == 100 and reading_enr.certificate_id is not None'''

good_block = '''    # Also check reading module progress via raw SQL (since it lacks an ORM model)
    from sqlalchemy import text as sa_text
    from app.extensions import db
    reading_enr = db.session.execute(
        sa_text("SELECT progress_percent, certificate_id FROM rdp_enrollment WHERE user_id = :uid LIMIT 1"),
        {"uid": current_user.id}
    ).fetchone()
    reading_completed = reading_enr is not None and reading_enr.progress_percent == 100 and reading_enr.certificate_id is not None'''

text = text.replace(bad_block, good_block)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
