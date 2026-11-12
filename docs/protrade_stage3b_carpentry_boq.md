# Stage 3B: Carpentry BOQ Price List

Carpentry was already registered as a selectable trade in Stage 3A. The existing
shop-scoped Price List route now displays Carpentry BOQ for that trade. Other
Price Lists retain their existing behavior. No quote/invoice routes changed.

The reusable ProTradeBoqItem structure stores owner, shop, trade, unique code,
description, unit and decimal material/labour rates. PostgreSQL generates the
total rate as material_rate + labour_rate. The form previews this total and
ignores any submitted total. Each, m, square metres and cubic metres are supported.
Codes are trimmed/uppercased and unique per shop/trade; duplicate additions do
not overwrite existing items. Explicit edits retain the item ID and validate
owner/shop/trade. Server validation and database constraints protect rates,
units and required fields. No sample prices or BOQ records are seeded.

Existing mech_catalog_parts records, mechanic pricing, global overrides and
functionality are preserved. Prior flat Carpentry price records, if any, remain
in that table; they are not automatically converted because material/labour
splits and units cannot be inferred. Carpentry now displays BOQ items instead.
Other trades can adopt this structure later; none receives a BOQ interface here.

Verification (2026-11-12):
- python tests/test_protrade_carpentry_boq.py: 6 BOQ tests and 5 register tests.
- python tests/test_protrade_price_list.py: 6 Price List tests and the same 5
  register tests. Flat-list trade isolation uses existing Panel Beater fixtures
  now that Carpentry intentionally has a separate BOQ presentation.
- python tests/test_protrade_price_migration.py: 6 migration tests.
- 23 distinct PostgreSQL tests passed (28 executions with repeated register tests).
- BOQ tests run the real migration and actual route bodies/model against temporary
  PostgreSQL tables; cover add/edit/view, exact decimal totals, duplicate codes,
  invalid input, owner/shop/trade isolation and database constraints.
- Actual targeted Alembic upgrade succeeded on verified localhost ait_local_db;
  protrade_boq_002 is recorded and all 6 existing mechanic catalog records/prices
  matched exactly before and after. Syntax and git diff checks pass.

## Required production migration

Apply protrade_boq_002 using Render's assigned runtime DATABASE_URL before
opening the Carpentry BOQ page. It depends only on protrade_price_001; do not
run upgrade heads because unrelated migration branches exist. This revision
creates only protrade_boq_items and its constraints. It does not alter or copy
mechanic catalog rows. Production application has not been verified here.

In Render Shell, from the deployed repository root:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_boq_002')"
```

Confirm success before opening the feature:

```sql
SELECT version_num FROM alembic_version WHERE version_num = 'protrade_boq_002';
SELECT to_regclass('public.protrade_boq_items');
```

The migration verifies database/user identity before DDL. It is tracked by
Alembic; do not execute its upgrade body repeatedly or manually stamp a revision
without applying its schema. Downgrade deliberately refuses destructive drops.
