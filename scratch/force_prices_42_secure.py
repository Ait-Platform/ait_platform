from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"
engine = create_engine(DB_URL)
with engine.begin() as conn:
    countries = conn.execute(text("SELECT alpha2, currency, fx_to_zar FROM ref_country_currency WHERE is_active = true")).fetchall()
    for c in countries:
        alpha2 = c[0]
        currency = c[1]
        fx_to_zar = float(c[2]) if c[2] else 0.0
        
        local_cents = 15000
        zar_cents = 15000
        
        if fx_to_zar > 0:
            computed_zar = int(local_cents * fx_to_zar)
            if computed_zar < 3000:
                local_cents = int(3000 / fx_to_zar)
                computed_zar = int(local_cents * fx_to_zar)
            zar_cents = computed_zar
        else:
            zar_cents = 3000
            
        if alpha2 == 'ZA':
            local_cents = 15000
            zar_cents = 15000
            
        # Hard constraint fallback
        if zar_cents <= 0: zar_cents = 3000
        if local_cents <= 0: local_cents = 3000
            
        conn.execute(text("""
            INSERT INTO subject_country_price (subject_id, country_code, local_currency, local_amount_cents, zar_amount_cents)
            VALUES (42, :cc, :curr, :lc, :zc)
            ON CONFLICT (subject_id, country_code) DO UPDATE 
            SET local_currency = EXCLUDED.local_currency,
                local_amount_cents = EXCLUDED.local_amount_cents,
                zar_amount_cents = EXCLUDED.zar_amount_cents
        """), {"cc": alpha2, "curr": currency, "lc": local_cents, "zc": zar_cents})
print("DB done securely")
