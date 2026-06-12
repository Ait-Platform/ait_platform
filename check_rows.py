import sys
from app import create_app, db
from app.admin.billing.water import get_consumption_rows_for_month

app = create_app()

with app.app_context():
    print("May:", [dict(r) for r in get_consumption_rows_for_month(3, '2026-05')])
    print("June:", [dict(r) for r in get_consumption_rows_for_month(3, '2026-06')])
