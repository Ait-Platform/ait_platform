# Stage 3A production price scope migration

Inspection found no Alembic reference to `mech_catalog_parts` before this fix.
The Stage 3A columns existed only in model metadata and a guarded startup helper.
The repository has multiple independent migration heads. Do not run unrelated
legacy upgrades to repair this table.

`protrade_price_001` is an independent, targeted PostgreSQL revision. It adds only
missing nullable `shop_id` and `trade` columns. The new shop_id column references
mech_shops(id). Existing columns, records, IDs, owners, prices and timestamps
are never rewritten. NULL scope continues to use Stage 3A's legacy mechanic
filtering. The revision safely tolerates the prior startup helper, partial
upgrades and equivalent manual SQL. Downgrade refuses destructive column drops.

Focused verification:
`.venv/Scripts/python.exe tests/test_protrade_price_migration.py`
Six PostgreSQL tests pass against transaction-temporary tables in verified local
ait_local_db. Tests cover both columns absent, each column independently absent,
repeat upgrade, exact legacy row/price preservation, required-table checks and
non-destructive downgrade.

Render verification, 2026-11-12: user executed the equivalent transaction in
Render psql and returned information_schema output confirming:
- shop_id: integer, nullable YES
- trade: character varying, nullable YES

This verifies the production SQL schema repair, not Alembic revision tracking.
No production dashboard was tested before that confirmation. No production
credentials were retrieved from scripts or exposed.

After this commit is deployed, run this command in the Render service shell to
execute/record only this revision, using the service's assigned DATABASE_URL:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location', 'migrations'); command.upgrade(c, 'protrade_price_001')"
```

The existing columns will be preserved. Confirm the revision is recorded with:

```sql
SELECT version_num FROM alembic_version WHERE version_num = 'protrade_price_001';
```

No Carpentry, BOQ, dashboard, filtering or unrelated functionality was modified.
