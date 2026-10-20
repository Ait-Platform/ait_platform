# Bank detail ID generation repair

The model declares an integer primary key with implicit SQLAlchemy autoincrement.
The banking INSERT omits id and therefore requires PostgreSQL identity or a
sequence-backed default. Checked-in migrations reference bil_bank_detail but
contain no creation or repair of its ID generator. The reported 23502/null-id
failure is consistent with the existing table lacking effective server generation.
Production catalogs have not been inspected in this task; a null-producing
default or trigger would require separate investigation.

Before approval, inspect only metadata in the verified production read-only session:

```sql
SELECT current_database(), current_user, current_schema();
SELECT a.attidentity,
       pg_get_expr(d.adbin,d.adrelid) AS id_default,
       pg_get_serial_sequence('public.bil_bank_detail','id') AS owned_sequence
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
WHERE a.attrelid='public.bil_bank_detail'::regclass
  AND a.attname='id' AND NOT a.attisdropped;
```

Use the independently verified application schema in place of public if different.
No bank values are read by this inspection.

After separate production approval, backup and identity/host/user/schema checks,
run exactly this command from the deployed checkout, using its verified production
DATABASE_URL (do not use local .env or paste credentials into command arguments):

```powershell
python -m alembic -c alembic.ini upgrade pm_bank_id_003
```

Prerequisite: pm_bank_register_002 is already applied. Check that revision first;
do not use this repair command to implicitly approve other pending migrations.
Do not upgrade all heads.

The migration locks only bil_bank_detail with a five-second lock timeout. It
checks the integer primary key, leaves an existing identity/default untouched,
restores an owned sequence default if one exists, or adds PostgreSQL identity
generation. The next generated value starts above existing IDs and never rewinds
an existing sequence. It changes no row IDs or bank data and touches no other
business table. Alembic records its revision normally. No application startup,
production execution, automatic historical imports or arbitrary ID assignment
is part of this change. A destructive downgrade is intentionally disabled.
