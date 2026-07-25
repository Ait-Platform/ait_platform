import sys
import os

# Ensure the app context is available
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app, db
from app.models.auth import AuthSubject
from app.models.payment import SubjectCountryPrice

def run_seed():
    app = create_app()
    with app.app_context():
        # 1. Create or update the AuthSubject
        hc_subj = AuthSubject.query.filter_by(slug='healthcore').first()
        if not hc_subj:
            hc_subj = AuthSubject(slug='healthcore', name='HealthCore', is_active=1)
            db.session.add(hc_subj)
            db.session.commit()
            print('Created AuthSubject healthcore')
        else:
            hc_subj.is_active = 1
            db.session.commit()
            print('Updated AuthSubject healthcore to active')

        # 2. Copy all pricing from cultural_fire
        cfi_subj = AuthSubject.query.filter_by(slug='cultural_fire').first()
        if not cfi_subj:
            print("Could not find cultural_fire subject. Pricing copy skipped.")
            return

        cfi_prices = SubjectCountryPrice.query.filter_by(subject_id=cfi_subj.id).all()
        count = 0
        for cp in cfi_prices:
            existing = SubjectCountryPrice.query.filter_by(subject_id=hc_subj.id, country_code=cp.country_code).first()
            if not existing:
                new_price = SubjectCountryPrice(
                    subject_id=hc_subj.id,
                    country_code=cp.country_code,
                    local_amount_cents=cp.local_amount_cents,
                    zar_amount_cents=cp.zar_amount_cents,
                    local_currency=cp.local_currency,
                    is_active=cp.is_active,
                    price_version=cp.price_version
                )
                db.session.add(new_price)
                count += 1
            else:
                existing.local_amount_cents = cp.local_amount_cents
                existing.zar_amount_cents = cp.zar_amount_cents
                existing.local_currency = cp.local_currency
                existing.is_active = cp.is_active
                existing.price_version = cp.price_version
        
        db.session.commit()
        print(f"Successfully copied/updated {len(cfi_prices)} pricing tiers to HealthCore.")

if __name__ == '__main__':
    run_seed()
