-- READ ONLY: no application factory, DDL, stamping, business data updates or R2 writes.
BEGIN READ ONLY;
SET LOCAL search_path TO public;
SET LOCAL statement_timeout='15s';
SELECT current_database(),current_user,current_schema(),current_setting('server_version');
SELECT table_name,column_name,data_type,is_nullable
FROM information_schema.columns WHERE table_schema='public'
 AND table_name IN ('user','bil_property','bil_lease_draft','bil_lease_document')
ORDER BY table_name,ordinal_position;
SELECT version_num FROM public.alembic_version ORDER BY version_num;
SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public'
 AND (tablename IN ('bil_lease_draft','bil_lease_document') OR indexname LIKE '%pm_lease%');
SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
 WHERE conrelid IN (to_regclass('public."user"'),to_regclass('public.bil_property'),
 to_regclass('public.bil_lease_draft'),to_regclass('public.bil_lease_document'));
SELECT has_schema_privilege(current_user,'public','CREATE') AS can_create_schema_objects,
 has_table_privilege(current_user,'public.bil_property','REFERENCES') AS property_references,
 has_table_privilege(current_user,'public."user"','REFERENCES') AS user_references;
SELECT id,name,manager_id,is_archived FROM public.bil_property WHERE id=26;
COMMIT;
