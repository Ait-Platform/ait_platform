BEGIN READ ONLY;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='3s';
SELECT current_database(),session_user,current_user,current_schema(),current_setting('search_path');
SELECT table_name,column_name,data_type,character_maximum_length,is_nullable,column_default
FROM information_schema.columns WHERE table_schema='public'
AND table_name IN ('bil_tenant','bil_tenant_identity') ORDER BY table_name,ordinal_position;
SELECT conrelid::regclass AS table_name,conname,convalidated,pg_get_constraintdef(oid)
FROM pg_constraint WHERE conrelid IN ('public.bil_tenant'::regclass,'public.bil_tenant_identity'::regclass)
ORDER BY conrelid,conname;
SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public'
AND tablename IN ('bil_tenant','bil_tenant_identity') ORDER BY tablename,indexname;
SELECT version_num FROM public.alembic_version ORDER BY version_num;
SELECT has_table_privilege(current_user,'public.bil_tenant','SELECT,INSERT') AS tenant_access,
has_table_privilege(current_user,'public.bil_tenant_identity','SELECT,INSERT') AS representative_access,
has_sequence_privilege(current_user,pg_get_serial_sequence('public.bil_tenant','id'),'USAGE') AS tenant_sequence,
has_sequence_privilege(current_user,pg_get_serial_sequence('public.bil_tenant_identity','id'),'USAGE') AS representative_sequence;
COMMIT;
