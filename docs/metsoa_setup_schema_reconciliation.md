# METSOA wizard schema reconciliation

Revision: metsoa_setup_001 (independent named Alembic branch).

The model declares bil_muni_account.property_id, but tracked migrations do not add it. The broad sync migration changes other account fields without adding this association. Restored navigation exposed older model/schema drift.

This migration adds only missing nullable optional fields declared by BilMuniAccount and BilMeter. It leaves existing column values, types, defaults and constraints unchanged. Newly added relationship columns receive foreign keys. No associations are inferred or backfilled: existing unassociated accounts remain NULL. Required base tables/identity columns, readings, consumption, owners and architecture-draft columns are checked before additions; missing prerequisites abort. Lock waiting is limited to ten seconds. Downgrade is blocked to avoid losing subsequently saved associations.

## Render procedure — approval required; not executed

After deployment, use the existing authenticated Render Shell in the repository root with its existing DATABASE_URL. Take the normal backup before the maintenance window.

    python -m alembic -c alembic.ini current
    python -m alembic -c alembic.ini upgrade metsoa_setup_001
    python -m alembic -c alembic.ini current

Use the named revision only, never head/heads or the broad sync migration. Do not stamp revisions or manually add columns. env.py loads metadata without calling create_app.

Verify metsoa_setup_001 appears among current revisions. Reopen /billing/setup?property_id=26 as the authorised manager; expect HTTP 200. Existing linked configurations remain view-only. Compare pre/post counts and existing values for accounts, meters, readings and consumption; previously unassociated accounts must remain NULL. Do not test Save in production.

## Verification limits

Isolated PostgreSQL tests reproduce the reported missing property_id and additional optional account/meter omissions, apply the migration, and render the actual wizard route/template. They verify preservation, NULL associations, foreign keys, repeat-safe additions, permissions and save protection. The complete production catalog and revision list were not supplied; no live connection was used. Tests match the confirmed drift, not an independently verified full production schema. Unexpected missing base prerequisites are reported rather than rebuilt.
