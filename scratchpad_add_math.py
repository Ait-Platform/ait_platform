from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject
from app.models.payment import SubjectCountryPrice

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='adv_math').first()
    if not subj:
        subj = AuthSubject(
            slug='adv_math',
            name='Advanced Mathematics',
            program_type='paid',
            commercial_mode='paid',
            is_active=1,
            requires_price=1,
            allow_country_pricing=1,
            start_endpoint='adv_math_bp.welcome'
        )
        db.session.add(subj)
        db.session.commit()
        print(f"Created subject adv_math with id {subj.id}")
    else:
        print(f"Subject adv_math already exists with id {subj.id}")
        subj.start_endpoint = 'adv_math_bp.welcome'
        db.session.commit()

    # Add pricing
    countries = ['ZA', 'US', 'GB', 'BW', 'NA', 'SZ', 'LS', 'MZ', 'ZM', 'ZW']
    for cc in countries:
        price = SubjectCountryPrice.query.filter_by(subject_id=subj.id, country_code=cc).first()
        if not price:
            p = SubjectCountryPrice(
                subject_id=subj.id,
                country_code=cc,
                local_amount_cents=199900 if cc == 'ZA' else 9900,
                zar_amount_cents=199900,
                local_currency='ZAR' if cc == 'ZA' else 'USD'
            )
            db.session.add(p)
    db.session.commit()
    print("Added pricing for adv_math")
