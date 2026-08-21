from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    thunee = AuthSubject.query.filter_by(slug='thunee').first()
    if not thunee:
        thunee = AuthSubject(
            name='Thunee Game',
            slug='thunee',
            is_active=1,
            show_on_welcome=True,
            is_hidden_on_bridge=False,
            requires_price=0,
            commercial_mode='free',
            enroll_policy='auto_enroll',
            processor_default='yoco',
            allow_country_pricing=0,
            mor_mode=0,
            program_type='free'
        )
        db.session.add(thunee)
        db.session.commit()
        print("Successfully added Thunee to AuthSubject")
    else:
        print("Thunee already exists")
