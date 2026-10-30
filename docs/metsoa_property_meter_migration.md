# Property-based municipal meters

Revision: metsoa_setup_002. Parent: metsoa_setup_001.

The existing application model and independent Municipal Accounts service already use meter.property_id, municipal_bill_number and account water/electricity meter pointers. Production's legacy sectional_unit_id NOT NULL constraint prevents inserts that omit sectional units. This migration removes only that NOT NULL requirement. It retains the column, historical values, foreign keys, indexes, accounts, readings and consumption. Installations without that legacy column need no column change. Downgrade is intentionally blocked because restoring NOT NULL would reject valid new meters.

## Render deployment and migration — approval required

Code is deployed by the existing Git/Render workflow. Until migration is applied, the existing schema safeguard continues to block meter creation safely. No production migration was executed during implementation.

After approval, take the normal database backup and use the existing authenticated Render Shell in the deployed repository root with its existing DATABASE_URL:

    python -m alembic -c alembic.ini current
    python -m alembic -c alembic.ini upgrade metsoa_setup_002
    python -m alembic -c alembic.ini current

Target this revision only, not head/heads. Do not use Flask startup, stamp revisions, or manually alter production tables. A ten-second lock timeout causes rollback if the meter table cannot be locked promptly; investigate and retry in a maintenance window.

Verify metsoa_setup_002 appears in current revisions. Read-only catalog verification:

    SELECT column_name,is_nullable FROM information_schema.columns
    WHERE table_schema='public' AND table_name='bil_meter'
      AND column_name IN ('property_id','sectional_unit_id');

    SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
    WHERE conrelid='public.bil_meter'::regclass ORDER BY conname;

Compare counts and original rows against the backup for bil_meter, bil_muni_account, bil_meter_reading and bil_consumption. sectional_unit_id must be nullable when present, with its existing FK preserved. No property assignments or historical values are backfilled.

## Verification limits

Focused isolated PostgreSQL tests reproduce the legacy mandatory column and FK, apply the real Alembic upgrade twice, check existing rows and foreign keys, create water/electricity meters for test Property 26 without sectional units, save baselines, and calculate next-month consumption. The existing suite covers permissions, account pointers, historical preservation and METSOA/Utilities access. Production catalog and deployment were not inspected live; no production accounts or readings were created.
