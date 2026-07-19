INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 10000, 10000, 'ZAR', true 
FROM auth_subject WHERE slug = 'tpx'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 2000, 2000, 'ZAR', true 
FROM auth_subject WHERE slug = 'cultural_fire'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 15000, 15000, 'ZAR', true 
FROM auth_subject WHERE slug = 'home'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 15000, 15000, 'ZAR', true 
FROM auth_subject WHERE slug = 'reading'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 15000, 15000, 'ZAR', true 
FROM auth_subject WHERE slug = 'loss'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 10000, 10000, 'ZAR', true 
FROM auth_subject WHERE slug = 'practice_crm'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 5000, 5000, 'ZAR', true 
FROM auth_subject WHERE slug = 'budget'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 7500, 7500, 'ZAR', true 
FROM auth_subject WHERE slug = 'billing'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 25000, 25000, 'ZAR', true 
FROM auth_subject WHERE slug = 'hds'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');

INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active)
SELECT id, 'ZA', 7500, 7500, 'ZAR', true 
FROM auth_subject WHERE slug = 'mechanic'
AND NOT EXISTS (SELECT 1 FROM subject_country_price WHERE subject_id = auth_subject.id AND country_code = 'ZA');