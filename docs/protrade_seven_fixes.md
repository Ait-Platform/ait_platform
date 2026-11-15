# ProTrade focused improvements — November 2026

No production migrations or live financial-data changes were performed during development.

1. BOQ catalogue selection reads the existing authorised user's Carpentry price catalogues. Multiple owned catalogues remain distinguishable; quotation adjustments never update source prices. Saved snapshots, section charging and preparation tracking are preserved. No migration required. Focused catalogue/quotation/BOQ/timing checks: 19 passed, including headless Chromium selection and total calculation.
2. Removed only the Work Activity action from the Debtor Register. Work history remains accessible from the ledger and is preserved. No migration required. Debtor isolation/history checks: 9 passed.
3. Darkened secondary ProTrade text in the existing templates and dynamic rows, preserving layouts and primary styles. No migration required. Relevant page, catalogue/browser and debtor tests: 13 passed.
4. Admin Hub → ProTrade Pricing reuses `universal_token_tariff` for token prices and adds an append-only `protrade_pricing_revision` audit snapshot. Defaults remain 10 tokens per ordinary quote/invoice/SOA, 25 per BOQ section, existing suggested customer prices and R50/R25/R25 BOQ terms. Admin approval applies prospectively; unchanged paid documents reuse their original receipts. Historical receipt immutability remains enforced. Paystack purchasing/verification is unchanged. The generic tariff page redirects ProTrade edits to the audited approval page; PostgreSQL rejects unaudited updates/deletions. BOQ commercial changes require explicit, internally consistent approval of all amounts together; nothing infers a new operator earning automatically. Help section 9 and document price labels use current approved values. Focused pricing, charging, timing and full-chain migration tests: 20 passed and 14 subtests passed.

## Required migration and deployment

Fix 4 requires `protrade_pricing_017` after `protrade_boq_time_016`. Earlier ancestry remains `012 → 014 → 013 → 015 → 016 → 017`. Migration 017 inserts the approved baseline tariff rows and an audit snapshot, retaining existing financial rows and numbering/ledger functions. It deliberately refuses conflicting pre-existing ProTrade tariff values; review such a conflict rather than stamping past it.

Use deployed files and the authenticated Render Shell with the running service's approved PostgreSQL configuration, following AGENT.md target verification. Do not use the local `.env` for production. Apply required migrations before exposing updated ProTrade pages. From the repository root, programmatic Alembic avoids the historical INI script-location error and application startup maintenance:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.current(c)"
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_pricing_017')"
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.current(c)"
```

Confirm the expected revision and restart the service. No migration was executed against production by the agent. Retain additive tables/audit history when rolling back application code; do not erase or rewrite financial history.
