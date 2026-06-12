from app.extensions import db
from app.models.auth import AuthSubject
from app.models.payment import SubjectCountryPrice
from sqlalchemy.sql import text

def add_cfi_judge():
    subj = AuthSubject.query.filter_by(slug='cfi_judge').first()
    if not subj:
        subj = AuthSubject(
            slug='cfi_judge',
            name='CFI Judge Application',
            program_type='lifelong',
            commercial_mode='paid',
            is_active=1
        )
        db.session.add(subj)
        db.session.commit()
        print(f"Created subject cfi_judge with id {subj.id}")
    else:
        print(f"Subject cfi_judge already exists with id {subj.id}")

    # Add pricing
    countries = ['ZA', 'US', 'GB', 'BW', 'NA', 'SZ', 'LS', 'MZ', 'ZM', 'ZW'] # Basic list
    for cc in countries:
        price = SubjectCountryPrice.query.filter_by(subject_id=subj.id, country_code=cc).first()
        if not price:
            p = SubjectCountryPrice(
                subject_id=subj.id,
                country_code=cc,
                local_amount_cents=5000, # 50 ZAR equivalent
                zar_amount_cents=5000,
                local_currency='ZAR' if cc == 'ZA' else 'USD', # simplified
            )
            db.session.add(p)
    db.session.commit()
    print("Added pricing.")

if __name__ == '__main__':
    from app import create_app
    app = create_app()
    with app.app_context():
        add_cfi_judge()
