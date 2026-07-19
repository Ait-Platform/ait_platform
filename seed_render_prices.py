from app import create_app, db
from app.models.auth import AuthSubject
from app.models.payment import SubjectCountryPrice, RefCountryCurrency

def seed_missing_za_prices():
    app = create_app()
    with app.app_context():
        # Prices in cents
        prices = {
            'cultural_fire': 2000,
            'hds': 25000,
            'tpx': 10000
        }

        print("Seeding base ZA prices for Render...")
        
        for slug, zar_cents in prices.items():
            subj = AuthSubject.query.filter_by(slug=slug).first()
            if not subj:
                print(f"Skipping {slug}: subject not found in DB.")
                continue

            za_price = SubjectCountryPrice.query.filter_by(subject_id=subj.id, country_code='ZA').first()
            if not za_price:
                # Need to find ZAR currency info
                c_ref = RefCountryCurrency.query.filter_by(alpha2='ZA').first()
                currency = c_ref.currency if c_ref else "ZAR"
                
                new_price = SubjectCountryPrice(
                    subject_id=subj.id,
                    country_code='ZA',
                    local_amount_cents=zar_cents,
                    zar_amount_cents=zar_cents,
                    local_currency=currency,
                    is_active=True
                )
                db.session.add(new_price)
                print(f"Inserted ZA base price for {slug} (Subject ID: {subj.id}) -> {zar_cents} cents")
            else:
                print(f"{slug} already has a ZA price (Subject ID: {subj.id}).")
        
        db.session.commit()
        print("Done seeding ZA base prices. The dynamic checkout logic will now work!")

if __name__ == '__main__':
    seed_missing_za_prices()
