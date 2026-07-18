from app import create_app, db
from sqlalchemy import text
import math

app = create_app()
with app.app_context():
    from app.models.auth import AuthSubject
    s = AuthSubject.query.filter_by(slug='cultural_fire').first()
    if not s:
        print('cultural_fire not found')
        exit()

    base_zar_cents = 2000
    
    # Get all active countries with their currencies and fx rates
    countries = db.session.execute(text("""
        SELECT alpha2, currency, fx_to_zar
        FROM ref_country_currency
        WHERE COALESCE(is_active, TRUE) = TRUE
    """)).mappings().all()

    inserted_count = 0
    updated_count = 0
    
    for c in countries:
        country_code = c['alpha2']
        currency = c['currency']
        fx_to_zar = c['fx_to_zar']
        
        if country_code == 'ZA':
            continue
            
        if fx_to_zar is None:
            continue
            
        local_amount_cents = int(round(base_zar_cents * float(fx_to_zar)))
        
        if local_amount_cents < 1:
            local_amount_cents = 1
        
        db.session.execute(text("""
            INSERT INTO subject_country_price
                (subject_id, country_code, local_currency, local_amount_cents, zar_amount_cents, is_active)
            VALUES
                (:sid, :cc, :cur, :local, :zar, TRUE)
            ON CONFLICT (subject_id, country_code)
            DO UPDATE SET
                local_currency     = EXCLUDED.local_currency,
                local_amount_cents = EXCLUDED.local_amount_cents,
                zar_amount_cents   = EXCLUDED.zar_amount_cents,
                is_active          = EXCLUDED.is_active
        """), {
            'sid': s.id,
            'cc': country_code,
            'cur': currency,
            'local': local_amount_cents,
            'zar': base_zar_cents
        })
        inserted_count += 1
        
    db.session.commit()
    print(f'Successfully generated {inserted_count} parity prices for CFI.')
