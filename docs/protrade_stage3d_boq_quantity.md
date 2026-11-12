# Stage 3D: Carpentry BOQ units and quantity

The Unit dropdown remains enabled on new and existing items. Description
selection still selects the predefined default unit, and the user may override
it with another supported unit; the server validates and saves that choice.

Quantity defaults to 1 and is shown in the form and BOQ register. Measured units
m, square metres and cubic metres support three decimal places; Hour also
supports decimals. Each requires whole-number quantities. Quantities must be
positive. Missing quantity defaults to 1 for a new record and preserves the
stored quantity for an existing record, so older form submissions are compatible.

Material and labour rates remain per-unit prices. The existing total_rate field
is still material_rate + labour_rate. The separate generated total_amount is
quantity * (material_rate + labour_rate), rounded to cents by PostgreSQL. The form
previews both unit rate and full amount; integer arithmetic gives the amount
preview the same positive-value rounding. The register shows quantity, unit rate
and full total independently. This does not add quote/invoice quantity behavior.

Migration protrade_boq_004 depends on protrade_boq_003. It adds quantity with a
non-null server default of 1, validation constraints, and generated total_amount.
No existing BOQ identifier, code, description, unit, rates, unit-rate total or
timestamp is changed. Existing amounts therefore equal their original unit-rate
totals. No mechanic catalog or owner/shop/trade isolation logic changes.

Verification (2026-11-12):
- tests/test_protrade_boq_quantity.py: 7 focused PostgreSQL checks including a
  headless Chromium add/edit check, measured-unit calculations, defaults,
  migration preservation, register display, validation, rounding and isolation.
- Existing BOQ, predefined catalog, Price List, register and price migration
  suites pass: 37 distinct checks in total, with repeated register executions.
- Actual targeted Alembic migration succeeded on verified localhost ait_local_db;
  original BOQ fields and mechanic catalog records compared exactly before/after.
- git diff --check passes.

## Required Render migration

Run from the deployed repository root using Render's assigned DATABASE_URL before
opening the updated BOQ page. Do not use upgrade heads.

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_boq_004')"
```

Confirm:

```sql
SELECT version_num FROM alembic_version WHERE version_num = 'protrade_boq_004';
SELECT count(*) FROM protrade_boq_items WHERE quantity IS NULL;
```

Production migration execution has not been verified in this task.
