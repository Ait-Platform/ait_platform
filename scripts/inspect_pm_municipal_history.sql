-- Read-only. Run in the authenticated Render PostgreSQL session, before approval.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SELECT current_database(), current_user;
SELECT a.id,a.property_id,p.manager_id,a.owner_id,a.water_meter_id,a.elec_meter_id
FROM bil_muni_account a LEFT JOIN bil_property p ON p.id=a.property_id
WHERE a.id=1;
SELECT id,manager_id FROM bil_property WHERE id=27;
-- Known account-ID-linked history. Zero meter pointers do not imply no history.
SELECT 'bil_muni_cycle_totals' AS source,count(*) AS records FROM bil_muni_cycle_totals WHERE account_id=1
UNION ALL SELECT 'bil_metsoa_cycle',count(*) FROM bil_metsoa_cycle WHERE account_id=1
UNION ALL SELECT 'bil_muni_payment',count(*) FROM bil_muni_payment WHERE account_id=1;
-- Number-only matches are evidence to review, NOT proof of account ownership.
SELECT 'bil_metro_reading_log:number_match' AS source,count(*) AS records
FROM bil_metro_reading_log WHERE lower(btrim(account_number))='70134280012'
UNION ALL SELECT 'bil_municipal_payment:number_match',count(*)
FROM bil_municipal_payment WHERE lower(btrim(metro_account_number))='70134280012';
-- Inventory every declared dependent FK, including tables outside the known list.
-- Return generated read-only counts for review/execution; this SELECT executes none.
SELECT c.conrelid::regclass AS dependent_table,c.conname,
       pg_get_constraintdef(c.oid) AS definition,
       CASE WHEN cardinality(c.conkey)=1 AND cardinality(c.confkey)=1 AND target.attname='id'
         THEN format('SELECT %L AS source,count(*) AS records FROM %s WHERE %I=1;',
                     c.conrelid::regclass::text,c.conrelid::regclass,source.attname)
       END AS read_only_count_query
FROM pg_constraint c
LEFT JOIN pg_attribute source ON source.attrelid=c.conrelid AND source.attnum=c.conkey[1]
LEFT JOIN pg_attribute target ON target.attrelid=c.confrelid AND target.attnum=c.confkey[1]
WHERE c.contype='f' AND c.confrelid='bil_muni_account'::regclass
ORDER BY 1,2;
ROLLBACK;
