from app import create_app, db
from sqlalchemy import text
app = create_app()
with app.app_context():
    from app.models.auth import AuthSubject
    for slug in ['loss', 'home', 'billing', 'adv_math', 'mechanic', 'cultural_fire']:
        s = AuthSubject.query.filter_by(slug=slug).first()
        if s:
            rows = db.session.execute(text("SELECT COUNT(*) FROM subject_country_price WHERE subject_id = :sid"), {'sid': s.id}).scalar()
            print(f'Subject {slug} has {rows} parity prices')
