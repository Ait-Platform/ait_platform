# PM SOA: targeted production migration approval runbook

Status: prepared and rehearsed locally; **not approved or executed in production**.

## Cause and configuration

The posting guard requires the metadata columns and three unique indexes introduced by pm_soa_001, along with compatible amount/ref/ID definitions and READ COMMITTED isolation. The reported message means these checks failed; it does not by itself prove that every new object is absent. Inspect the actual catalog before approval.

Use the production AIT web service's authenticated Render Shell, at the deployed repository root containing the root alembic.ini. Use its already-assigned DATABASE_URL after independently matching it to the authoritative Render PostgreSQL resource. Do not paste credentials into chat, use old source-code connection strings, change local .env or introduce another connection system.

The root alembic.ini points to migrations/. migrations/env.py reads DATABASE_URL, normalises postgres://, and supports PostgreSQL only. It imports model definitions, but does not call create_app(). Do not use flask db upgrade: Flask application startup has unrelated maintenance. Do not use the nested migrations/alembic.ini, which would resolve a different script path.

pm_soa_001 is an independent root branch with no parent/dependency. Targeting it adds its version row alongside existing heads. Do not use upgrade head/heads, stamp, merge or run unrelated revisions.

## Prerequisites before execution approval

1. Confirm the deployed checkout includes pm_soa_001 and the reviewed UI release. Independently match the assigned database resource/host and current database role in Render. In a protected read-only SQL session verify current_database() is ait_platform_db, current_user is the assigned role, the active schema is public, and the expected search_path. Never display the URL.
2. Capture existing public.alembic_version rows and verify all revisions exist in this checkout. Confirm pm_soa_001 is absent. If it is already recorded but objects are missing, stop for investigation rather than stamp or rerun.
3. Inspect public.bil_tenant_ledger columns, its indexes, and any sequences named pm_ledger_debit_ref_seq / pm_ledger_credit_ref_seq. Base schema must match the reviewed migration/service. All three new metadata columns, three new indexes and two sequences should be absent for a first application. If only some exist, stop: this migration deliberately does not repair partial/manual installations.
4. Check duplicate historical opening balances (kind opening_balance or trimmed, case-insensitive description Opening Balance) and duplicate exact D/C four-digit references. The migration refuses these conditions without changing the rows. Confirm the migration role can alter the ledger/create objects in public and maintain alembic_version; the application role must be able to use both sequences and read/write the new columns.
5. Follow the established procedure: take a verified production backup including schema, financial records and alembic_version; rehearse on an isolated restored copy. Record original ledger columns and row counts/checksums for comparison. Schedule a quiet window because the migration takes an exclusive ledger lock.
6. Obtain explicit approval for this targeted production migration. None of these instructions authorises execution.

## Exact command for approval — do not run yet

From that verified production service repository root, in Render's Linux shell:

```bash
PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c search_path=public -c lock_timeout=5000 -c statement_timeout=120000" python -m alembic -c alembic.ini upgrade pm_soa_001
```

PGOPTIONS is command-scoped PostgreSQL session configuration, not a replacement database URL or persisted service setting. It bounds lock acquisition to 5 seconds and statements to 120 seconds and selects public explicitly. If these timeouts interrupt migration, inspect its transaction result; do not manually add objects or stamp success. Run in a session free of other concurrent migration commands.

This exact Alembic configuration, environment and target were rehearsed via Alembic's command.upgrade API on session-temporary local PostgreSQL tables. The rehearsal verified one added revision, unchanged original financial columns, preserved unrelated heads, correctly initialised sequences and a no-op second upgrade. This is not a production-compatibility certificate; current production schema, privileges, revision set, backup and restored-copy rehearsal remain approval prerequisites.

## Read-only success verification

In the existing authorised PostgreSQL client, run reviewed SELECT statements only:

```sql
SELECT version_num FROM public.alembic_version ORDER BY version_num;

SELECT column_name, data_type, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_schema='public' AND table_name='bil_tenant_ledger'
  AND column_name IN ('posting_key','posting_hash','posting_notes')
ORDER BY column_name;

SELECT i.relname AS index_name, x.indisunique, x.indisvalid,
       pg_get_indexdef(i.oid) AS definition
FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
WHERE x.indrelid='public.bil_tenant_ledger'::regclass
  AND i.relname IN ('uq_pm_ledger_posting_key','uq_pm_ledger_opening','uq_pm_ledger_dc_ref')
ORDER BY i.relname;

SELECT sequencename, min_value, max_value, cycle, last_value
FROM pg_sequences WHERE schemaname='public'
  AND sequencename IN ('pm_ledger_debit_ref_seq','pm_ledger_credit_ref_seq')
ORDER BY sequencename;
```

Expect:
- pm_soa_001 plus every previously recorded head; no unrelated revision movement.
- Nullable posting_key varchar(32), posting_hash varchar(64), posting_notes text.
- Three valid unique indexes: posting_key; tenant_id for the opening predicate; ref for the exact D/C four-digit predicate. Review definitions, not just names.
- Both sequences, minimum 1, maximum 9999, cycle false. Initial sequence position follows existing matching references; an uncalled sequence's last_value can appear NULL in pg_sequences. Do not call nextval merely to test it.
- Original financial row counts and original-column checksums match the pre-migration capture; existing references remain unchanged.
- The same PM tenant ledger still opens under existing access controls. Posting-schema rejection should clear for a genuinely compatible schema, but do not post a production test transaction. The first real authorised posting must obtain a unique reference through the normal form.

There is no destructive automatic downgrade. Preserve additive objects and financial history if a later application rollback is needed; any database restoration requires its own reviewed approval.
