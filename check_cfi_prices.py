from app import create_app, db
app = create_app()
with app.app_context():
    from sqlalchemy import text
    rows = db.session.execute(text("SELECT * FROM subject_country_price WHERE subject_id = (SELECT id FROM auth_subject WHERE slug='cultural_fire')")).fetchall()
    for r in rows:
        print(f"Country: {r.country_code}, Local Cents: {r.local_amount_cents}, ZAR Cents: {r.zar_amount_cents}, Active: {r.is_active}")
    if not rows:
        print('No country prices found for cultural_fire')
