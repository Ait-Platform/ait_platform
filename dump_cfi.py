from app import create_app, db
from app.models.payment import SubjectCountryPrice

app = create_app()
with app.app_context():
    rows = SubjectCountryPrice.query.filter_by(subject_id=12).all()
    with open('cfi_render_insert.sql', 'w') as f:
        f.write('INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, price_version, is_active)\nVALUES\n')
        values = []
        for r in rows:
            values.append(f"(12, '{r.country_code}', {r.local_amount_cents}, {r.zar_amount_cents}, '{r.local_currency}', '{r.price_version}', true)")
        f.write(',\n'.join(values) + '\nON CONFLICT (subject_id, country_code) DO UPDATE SET local_amount_cents = EXCLUDED.local_amount_cents, zar_amount_cents = EXCLUDED.zar_amount_cents;')
    print(f'Successfully exported {len(rows)} rows to cfi_render_insert.sql')
