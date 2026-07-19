INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 10000, 10000, 'ZAR', true 
FROM auth_subject WHERE slug = 'tpx'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 2000, 2000, 'ZAR', true 
FROM auth_subject WHERE slug = 'cultural_fire'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 15000, 15000, 'ZAR', true 
FROM auth_subject WHERE slug = 'home'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 15000, 15000, 'ZAR', true 
FROM auth_subject WHERE slug = 'reading'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 15000, 15000, 'ZAR', true 
FROM auth_subject WHERE slug = 'loss'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 10000, 10000, 'ZAR', true 
FROM auth_subject WHERE slug = 'practice_crm'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 5000, 5000, 'ZAR', true 
FROM auth_subject WHERE slug = 'budget'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 7500, 7500, 'ZAR', true 
FROM auth_subject WHERE slug = 'billing'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 25000, 25000, 'ZAR', true 
FROM auth_subject WHERE slug = 'hds'
ON CONFLICT DO NOTHING;

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 7500, 7500, 'ZAR', true 
FROM auth_subject WHERE slug = 'mechanic'
ON CONFLICT DO NOTHING;