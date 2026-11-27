# ProTrade unified construction Price List

The canonical construction price model is the existing `ProTradeBoqItem` /
`protrade_boq_items`. There is no third pricing table. The existing
`boq_price_list.html` interface now serves Carpentry, Plumbing, Painting,
Electrical, Building, General Works and BOQ clients. The dashboard keeps its
shop-specific Price List action beside Debtor Register.

`app/protrade_boq_catalog.py` owns unpriced item definitions, category metadata,
client-scoped quotation choices, decimal validation and the shared line amount
calculation. Carpentry's original definitions and codes remain unchanged. New
standard construction definitions use a `STD` code segment to avoid assigning
new meanings to existing sequential client codes. Custom codes are generated
once and survive description/rate changes. More construction categories can be
registered without another management page or quotation engine.

Individual contractors see their registered category and General Works. BOQ
clients see all construction categories. Earlier BOQ custom items remain in their
original category and keep their IDs/codes. Category selection reads templates
without inserting prices. New clients have no compulsory rates. All saved prices
are scoped by authenticated consultant, selected shop and category. A BOQ NEVER
reads another shop's prices, even when both shops belong to the same consultant.

The quotation builder, saved-item editor and Price List Output use the same
canonical architecture. Quantity defaults to one; quotation rate adjustments do
not save back to the Price List. Saved snapshots remain authoritative for earlier
quotations and invoices, including earlier snapshots referencing another owned
catalogue. Starting a new quotation uses only the selected client's current
catalogue. Numbering, VAT, section generation, document charges and payments
retain their existing workflows.

## Legacy combined prices

Revision `protrade_unified_prices_021` extends the existing BOQ item table with
nullable `combined_rate` and a unique `legacy_part_id` provenance link. An item
has either material/labour rates or an explicit combined rate, never both.
Generated totals and the shared calculator support both bases. Split rates use
two decimals, quantities three; an existing combined rate retains its full value
and only the line amount is rounded to cents (half up).

Eligible owner/shop-scoped construction rows in `mech_catalog_parts` are imported
once into the canonical table. Their codes, descriptions, prices and timestamps
are retained; the original rows remain unchanged. Existing flat catalogues had
no unit field; the previous adapter's Each unit is retained. Missing codes use
the earlier adapter's stable PREFIX-CAT-ID form. No material/labour split is
invented. Management, quotation previews, snapshot edits and Output label these
rates as combined. A consultant may explicitly choose to enter a material/labour
split for a client; this requires both rates and never guesses an allocation or
changes the retained source record. Existing BOQ rows retain their IDs, fields and values.
Mechanic and other legacy non-construction catalogues remain in their original
system and are not imported or changed.

A duplicate code, invalid price, missing ownership or incompatible source data
aborts the migration transaction for explicit reconciliation; it never silently
merges or discards records. Do not resolve such failures by deleting history.

## Deployment migration

Production migration execution has NOT been authorised or performed. The new
schema must be installed before serving the updated construction pricing code.
Coordinate the migration with application deployment; merely pushing code does
not establish that the schema is ready.

Target ONLY `protrade_unified_prices_021`, using the service's authoritative
runtime DATABASE_URL after independently verifying database and user identity.
Do not use `upgrade heads`. The revision branches from `protrade_trade_codes_019`
and does not depend on or execute the held folder/email revision 020. An already
installed 020 is preserved as an independent Alembic head, as are unrelated heads.
No changes to Folder Management or Franchise are included.

From the deployed repository root, after production approval:

```python
from alembic.config import Config
from alembic import command
c = Config('migrations/alembic.ini')
c.set_main_option('script_location', 'migrations')
c.attributes['skip_app_metadata'] = True
command.upgrade(c, 'protrade_unified_prices_021')
```

The migration is forward-only. Alembic records successful execution; another
targeted upgrade is a no-op. Do not call upgrade bodies repeatedly or manually
stamp the revision without applying its schema.

## Verification

Focused PostgreSQL fixtures use temporary tables and rollback. They exercise the
actual model, route bodies, production migration, Output adapter and Chromium
builder. Checks cover shared interfaces/default categories, General Works,
client/consultant isolation, same-owner cross-client forgery, stable codes,
duplicate prevention, exact combined-rate calculations, snapshot independence,
original financial-row preservation and transactional migration collision rollback.
Operational Alembic verification checks preservation of unrelated heads and
repeat-upgrade behavior. Relevant Mechanic, VAT, token, Output and historical
item-edit regressions accompany these checks.

Final release suite: **254 tests and 52 subtests passed**, with 12 documented
legacy fixture tests deselected. The latest Output/numbering check also passed
23 tests and 6 subtests. Python syntax and `git diff --check` passed.

PowerShell release command (from the repository root):

```powershell
$protradeTestFiles = Get-ChildItem -LiteralPath tests -Filter 'test_protrade*.py' | ForEach-Object { $_.FullName }
& .\.venv\Scripts\python.exe -m pytest $protradeTestFiles -q -k 'not ProTradeDashboardTests and not test_release_menu_legacy_redirect_and_ownership' --tb=short --disable-warnings
```

Live Render migration/deployment and authenticated browser verification remain
separate. Automated Chromium checks do not establish live Windows verification.

The persistent local database was inspected read-only as `ait_local_db` /
`ait_local`. Its recorded ProTrade revision is still 012. It was NOT advanced:
the earlier outstanding migration chain includes token/financial features outside
this pricing task. Tests install the required schemas only in isolated fixtures.
Local interactive use also needs a separately coordinated prerequisite upgrade.

PARKED pre-existing test-fixture debt: `ProTradeDashboardTests` copies old route
logic (already excluded by the established regression command). The test
`DashboardSupportTests.test_release_menu_legacy_redirect_and_ownership` expects
the retired Price List navigation and folder redirect; HEAD already has the
shop-row Price List action and local-folder page instead. That obsolete test is
excluded from the focused release suite. Neither the held folder implementation
nor the obsolete folder test is changed by this release.

## Changed files

- `app/models/mechanic.py`
- `app/program_mechanic/routes.py`
- `app/protrade_boq_catalog.py`
- `app/protrade_output.py`
- `app/protrade_sections.py`
- `app/static/js/carpentry_quote_builder.js`
- `docs/protrade_unified_price_list.md`
- `migrations/versions/protrade_unified_prices_021.py`
- `templates/program_mechanic/boq_price_list.html`
- `templates/program_mechanic/carpentry_quote.html`
- `templates/program_mechanic/carpentry_quote_items.html`
- `templates/program_mechanic/output_document.html`
- `tests/test_protrade_boq_catalogue.py`
- `tests/test_protrade_boq_quantity.py`
- `tests/test_protrade_carpentry_boq.py`
- `tests/test_protrade_carpentry_catalog.py`
- `tests/test_protrade_carpentry_quote_hub.py`
- `tests/test_protrade_carpentry_quote_inputs.py`
- `tests/test_protrade_carpentry_quote_state.py`
- `tests/test_protrade_construction_codes.py`
- `tests/test_protrade_quote_numbering.py`
- `tests/test_protrade_shop_register.py`
- `tests/test_protrade_trade_quotes.py`
- `tests/test_protrade_unified_prices.py`
