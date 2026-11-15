# ProTrade five focused corrections — 15 November 2026

No production migrations or live financial records were changed by the agent. Existing BOQ commercial amounts, Paystack arrangements, quotation numbering, section charges and preparation tracking remain in place.

| Fix | Implementation and principal files |
| --- | --- |
| 1 — Folder selection | `protrade_output.js` separates selecting/displaying a handle from explicit Save and persistence. Permission is requested from the Save gesture; storage/permission failures retain the selected name and previous saved assignment, with an accurate error rather than blanket cancellation. `document_folder_history.js`, `pm_local_directories.js` and the existing folder-control/history templates automatically list available saved PDFs and refresh after successful saves, including across tabs. ProTrade no longer shows Refresh Files. Existing IndexedDB assignments, lazy directories, original files and native View / Print are reused. PM retains its existing interface. |
| 2 — Multiple trades | `protrade_boq_catalog.py`, the existing quote route/builder and `carpentry_quote.html` read authorised Carpentry, Plumbing, Electrical, Painting and Building catalogues directly from existing models. Multiple catalogues can be selected together. Source IDs/trades are retained, quotation rates are editable without changing catalogue prices, and trade subtotals feed one quotation total. Legacy combined catalogue rates populate the material/catalogue-rate column with zero additional labour, clearly labelled; no material/labour split is invented or catalogue record duplicated. |
| 3 — Plumbing codes | Existing `MechCatalogPart` rows gain one nullable `item_code` field. Migration 019 assigns construction prefixes to eligible catalogue rows without changing their IDs, descriptions or prices. Plumbing uses PLUM-001, PLUM-002, etc. Allocations are serialized by the owned shop and reserve codes found in saved quotations. Existing historical WORK-001 and other saved document codes remain unchanged. The same adapter supports other construction prefixes. No independent item system is introduced. |
| 4 — Invoice VAT display | Only `output_document.html` rendering changes. Zero-valued VAT/tax amount rows are omitted from invoices; positive VAT remains. Zero-rated tax invoices retain their treatment, rate, legal basis and supplier/recipient information. Calculations and stored snapshot amounts are unchanged. Previously saved PDF bytes are not rewritten. |
| 5 — Shop Profile navigation | The dashboard's Shop Name links to its existing `action=setup` profile entry point with the exact shop ID. The existing editable modal and populated fields are reused. Debtor Register remains a separate action. Invalid/foreign explicit shop IDs are rejected instead of falling back to another shop. No profile rows are duplicated. |

## Verification

Focused checks covered real browser DirectoryHandles/IndexedDB, explicit folder Save/retry, permission denial, automatic history, historical PDF retrieval, failed writes, BOQ saves without additional tokens/timer finalisation, multi-trade browser selection/subtotals, unchanged catalogue prices, stable sequential codes, full migration ancestry, immutable historical rows, zero/positive VAT rendering and profile navigation/ownership.

Final combined regression result: **252 tests and 46 subtests passed; no failures or skips**. Focused selections passed 21 folder/shared tests (with the corrected retry test also rerun), 20 catalogue/quotation/timer tests, 18 code/pricing/migration tests, 21 VAT/Output tests and 16 profile/debtor tests. The full final run supersedes intermediate fixture-expectation failures.

The maintained regression selection excludes the previously documented stale copied `ProTradeDashboardTests` fixture. PM integration checks use the explicitly verified local PostgreSQL connection via `PM_LEASE_TEST_DATABASE_URL`; credentials are not logged or persisted.

## Required migration and Render procedure

Only Fix 3 requires a new migration: **`protrade_trade_codes_019`**, after `protrade_vat_invoices_018`. Full relevant ancestry remains `012 → 014 → 013 → 015 → 016 → 017 → 018 → 019`. Persistent sequential codes need the additional column on the existing catalogue; there is no new catalogue, debtor, ledger or invoice table.

Use the new release files and the current authorised Render service/database configuration. Apply the additive migration in an approved pre-deploy step before starting workers that query the new column. From the repository root:

```sh
python -c "import os; from sqlalchemy import create_engine,text; e=create_engine(os.environ['DATABASE_URL']); c=e.connect(); print(c.execute(text('SELECT current_database(),current_user')).one()); c.close()"
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.current(c)"
```

Independently compare the reported database/user with the current Render resource and stop on a mismatch. Once production migration execution is approved, use:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_trade_codes_019'); command.current(c)"
```

Confirm revision 019, then start/restart updated workers. No production command above was executed during development. Do not stamp past revisions or run application startup maintenance as a substitute for Alembic. Retain published codes and historical records if rolling application code back.

## Browser limits

Direct folder access needs supported Chrome/Edge on a secure connection and browser permission. Windows/browser security may reject protected locations; choose a dedicated ProTrade subfolder and approve its access. Selection cancellation and persistence failure now have distinct messages. Folder assignments remain browser/device-specific. The automated tests use genuine sandboxed DirectoryHandles and IndexedDB with a substituted chooser, so an actual Windows picker/production-browser smoke test remains a deployment check. Unsupported browsers retain PDF download; viewing/printing historical PDFs does not regenerate or rewrite them.
