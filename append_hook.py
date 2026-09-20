with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

hook = """
@uip_bp.before_app_request
def auto_patch_db():
    from flask import request
    if request.endpoint and 'static' not in request.endpoint:
        from sqlalchemy import text
        from app.extensions import db
        try:
            db.session.execute(text("ALTER TABLE uip_organogram_seat ADD COLUMN IF NOT EXISTS duty VARCHAR(50) DEFAULT 'committee_member';"))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
"""

if "def auto_patch_db" not in text:
    with open("app/program_uip/routes.py", "a", encoding="utf-8") as f:
        f.write("\n" + hook)
    print("Hook appended to routes.py")
