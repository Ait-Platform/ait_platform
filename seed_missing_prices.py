from app import create_app, db
from app.models.payment import SubjectCountryPrice, RefCountryCurrency
from app.models.auth import AuthSubject
from app.payments.pricing import fx_rate_local_to_zar

app = create_app()

with app.app_context():
    subjects = AuthSubject.query.all()
    active_countries = RefCountryCurrency.query.filter_by(is_active=True).all()
    
    for subject in subjects:
        # Check how many prices exist
        count = SubjectCountryPrice.query.filter_by(subject_id=subject.id).count()
        if count < len(active_countries):
            # Find the base ZA price
            za_price = SubjectCountryPrice.query.filter_by(subject_id=subject.id, country_code='ZA').first()
            if not za_price:
                print(f"Skipping {subject.slug} because it has no ZA price to use as base.")
                continue
                
            base_zar_cents = za_price.zar_amount_cents
            print(f"Seeding missing prices for {subject.slug} (Base ZAR: {base_zar_cents})")
            
            for c in active_countries:
                exists = SubjectCountryPrice.query.filter_by(subject_id=subject.id, country_code=c.alpha2).first()
                if not exists:
                    fx = fx_rate_local_to_zar(c.alpha2)
                    local_cents = int(base_zar_cents * fx) if fx else base_zar_cents
                    local_currency = c.currency if fx else "ZAR"
                    
                    new_price = SubjectCountryPrice(
                        subject_id=subject.id,
                        country_code=c.alpha2,
                        local_amount_cents=local_cents,
                        zar_amount_cents=base_zar_cents,
                        local_currency=local_currency,
                        is_active=True
                    )
                    db.session.add(new_price)
            
            db.session.commit()
            print(f"Completed seeding for {subject.slug}.")
