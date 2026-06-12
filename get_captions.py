from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    res = db.session.execute(text("""
        SELECT id, caption FROM rdp_lesson ORDER BY "order" ASC
    """)).fetchall()
    
    for r in res:
        print(f"ID {r.id}: {repr(r.caption)}")
