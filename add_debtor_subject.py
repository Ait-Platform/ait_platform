from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subject = AuthSubject.query.filter_by(slug='debtors').first()
    if not subject:
        subject = AuthSubject(
            slug='debtors',
            name='Debtors & Statements',
            program_type='paid',
            commercial_mode='paid',
            requires_price=1,
            is_active=1
        )
        db.session.add(subject)
        db.session.commit()
        print('Added debtors subject.')
    else:
        print('Debtors subject already exists.')
