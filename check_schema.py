from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    res = db.session.execute(text("""
        SELECT column_name, column_default 
        FROM information_schema.columns 
        WHERE table_name='rdp_enrollment'
    """)).fetchall()
    for r in res:
        print(f"{r.column_name}: {r.column_default}")
