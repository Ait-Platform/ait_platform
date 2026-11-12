# Stage 3C: predefined Carpentry BOQ work items

The existing Carpentry BOQ Price List now provides a searchable native description
dropdown (datalist) with the requested 15 work items, CAR-001 through CAR-015.
Selection automatically sets code and unit. The server independently resolves
predefined codes/descriptions and units; client-submitted manual codes and units
cannot alter predefined definitions. The reusable trade-keyed catalog lives in
app/protrade_boq_catalog.py, without database seed rows or any default prices.

Users may type a custom description and choose its unit. Custom items receive a
server-generated CAR-C- UUID code (38 characters), enforced by the existing
shop/trade uniqueness constraint. An edit retains its code unless the user
explicitly chooses a predefined item. Unchanged legacy descriptions are not
silently matched/reclassified: an existing code/unit and prices survive reads
and rate-only saves even if the description happens to match a new predefined
item. Existing duplicate code conflicts produce an error rather than overwriting
another record. Owner/shop/trade checks remain in force.

The new Hour unit requires protrade_boq_003, a constraint-only migration depending
on protrade_boq_002. It replaces the unit check with the same existing units plus
Hour. No record, price, code, timestamp or calculated-total expression is changed.
Quotations, invoices and mechanic pricing were not modified.

Verification (2026-11-12):
- 30 distinct focused tests passed (35 executions including repeated register
  tests): 7 new catalog tests, 6 BOQ tests, 6 Price List tests, 5 register tests,
  and 6 price-scope migration tests.
- PostgreSQL tests cover all 15 mappings, Hour storage, unique custom codes,
  duplicate handling, server validation, owner/shop/trade isolation, and exact
  legacy record preservation.
- A headless Chromium check uses the real rendered form to verify automatic
  code/unit selection, custom mode and live totals.
- Targeted local Alembic upgrade succeeded on verified localhost ait_local_db;
  all existing catalog/BOQ fields compared exactly before and after.
- git diff --check passed.

Run tests directly with .venv/Scripts/python.exe:
- tests/test_protrade_carpentry_catalog.py
- tests/test_protrade_carpentry_boq.py
- tests/test_protrade_price_list.py
- tests/test_protrade_price_migration.py

## Production migration

After deployment, run this in Render Shell using the assigned runtime database
configuration before saving Hour items. Do not use upgrade heads.

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_boq_003')"
```

Production migration execution has not been verified in this task.
