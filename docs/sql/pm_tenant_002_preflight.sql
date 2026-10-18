BEGIN READ ONLY;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='3s';
SELECT current_database(),session_user,current_user,current_schema(),current_setting('search_path');
SELECT version_num FROM public.alembic_version ORDER BY version_num;
SELECT 'bil_tenant' AS table_name,count(*) AS records FROM public.bil_tenant
UNION ALL SELECT 'bil_tenant_identity',count(*) FROM public.bil_tenant_identity;
SELECT table_name,column_name,data_type,character_maximum_length,is_nullable,column_default
FROM information_schema.columns WHERE table_schema='public' AND table_name IN ('bil_tenant','bil_tenant_identity')
ORDER BY table_name,ordinal_position;
SELECT conrelid::regclass AS table_name,conname,convalidated,pg_get_constraintdef(oid)
FROM pg_constraint WHERE conrelid IN ('public.bil_tenant'::regclass,'public.bil_tenant_identity'::regclass)
ORDER BY conrelid,conname;
SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename IN ('bil_tenant','bil_tenant_identity') ORDER BY tablename,indexname;
SELECT has_schema_privilege(current_user,'public','USAGE') AS public_usage,
has_table_privilege(current_user,'public.bil_tenant','SELECT,INSERT') AS tenant_access,
has_table_privilege(current_user,'public.bil_tenant_identity','SELECT,INSERT') AS representative_access;
SELECT relname,pg_get_userbyid(relowner) AS table_owner,
pg_has_role(current_user,relowner,'USAGE') AS inherits_owner_role
FROM pg_class WHERE oid IN ('public.bil_tenant'::regclass,'public.bil_tenant_identity'::regclass);
COMMIT;
