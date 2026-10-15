-- READ ONLY. Run against the PostgreSQL resource used by the live AIT service.
-- psql service=approved_production -X -v ON_ERROR_STOP=1 -f sql/pm_lease_preflight.sql
BEGIN READ ONLY;
SET LOCAL search_path TO public;
SET LOCAL statement_timeout='15s';
SELECT current_database(),current_user,current_schema(),current_setting('server_version');
SELECT expected.column_name,actual.data_type,
       CASE WHEN actual.column_name IS NULL THEN 'MISSING' ELSE 'present' END AS state
FROM (VALUES ('id'),('property_id'),('start_date'),('end_date'),('rent_amount')) expected(column_name)
LEFT JOIN information_schema.columns actual ON actual.table_schema='public'
 AND actual.table_name='bil_lease' AND actual.column_name=expected.column_name
ORDER BY expected.column_name;
SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns
 WHERE table_schema='public' AND table_name IN ('bil_lease','bil_property','bil_tenant')
 ORDER BY table_name,ordinal_position;
SELECT version_num FROM public.alembic_version ORDER BY version_num;
SELECT id,name,manager_id,is_archived FROM public.bil_property WHERE id=26;
SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid=to_regclass('public.bil_lease');
COMMIT;
