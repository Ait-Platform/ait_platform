# UIP Phase 10: recorded Finance and member transparency

Implemented locally on 8 September 2026. Not deployed. Production has not been migrated.

## Financial year

Every standard Finance page and export uses 1 March through the last day of February, labelled YYYY/YY. 2026/27 ends 28 February 2027; 2027/28 ends 29 February 2028. The selected start year is explicit. Transaction filters can request an explicit cross-year date range; CSV output states that range.

## Added tables and migration

- `uip_finance_transaction` (`UipFinanceTransaction`): posted income, expenditure and signed adjustments, separate approved member purpose/party, provenance, optional scoped links, immutable reversals and replacements, publication state and request deduplication.
- `uip_finance_commitment` (`UipFinanceCommitment`): approved spending and current OPEN / PARTIALLY_PAID / PAID / CANCELLED state.
- `uip_finance_commitment_revision` (`UipFinanceCommitmentRevision`): immutable effective-dated amount/cancellation snapshots and management reasons.
- `uip_finance_budget_line` (`UipFinanceBudgetLine`): organisation, March financial-year start and category.
- `uip_finance_budget_revision` (`UipFinanceBudgetRevision`): immutable approved amounts, approval dates, reasons, actor and optional decision.

All monetary columns use Numeric(16,2); services use Decimal, reject floats/non-finite values, and require exact cents. Derived financial totals are not persisted.

One additive PostgreSQL migration: `uip_p10_finance`, directly after `uip_p49_operations`. Full UIP chain: `uip_p2_prod_base -> a27c9e4b6102 -> uip_p3_work_orders -> uip_p49_operations -> uip_p10_finance`. The unrelated legacy head `7da57fffdba9` remains separate.

The migration creates only Finance tables, indexes, constraints, functions and triggers. Composite foreign keys enforce same-organisation relationships. Posted monetary history and budget/commitment revision rows cannot be rewritten/deleted. Only transaction publication/version fields can change. Downgrade refuses to discard financial history. The migration was exercised only in generated disposable local schemas; do not apply it to production as part of this handoff.

## Endpoints

All routes have prefix `/uip/<org_slug>` and endpoint prefix `uip_bp.`.

| Path | Endpoint | Methods |
| --- | --- | --- |
| `/finance` | `finance_overview` | GET |
| `/finance/transactions` | `finance_transactions` | GET |
| `/finance/transactions/new` | `finance_transaction_new` | GET, POST |
| `/finance/transactions/<transaction_id>` | `finance_transaction` | GET, POST |
| `/finance/budget` | `finance_budget` | GET, POST |
| `/finance/commitments` | `finance_commitments` | GET, POST |
| `/finance/commitments/<commitment_id>` | `finance_commitment` | GET, POST |
| `/finance/reports/<report>.csv` | `finance_report` | GET |

Reports: transactions, income-expenditure, budget, commitments, category, provider. CSV text cells neutralise spreadsheet formula injection. Amounts remain numeric. Exports state the financial year/date range and whether they contain management or approved member records.

## Permissions and privacy

Existing active organisation membership and roles remain authoritative. Managers administer Finance. Owners, residents and committee members read approved transparency records only. Receptionist/provider roles alone grant no Finance access; a provider does not gain Finance access through work assignment. No new role is provisioned.

Writes require CSRF and server-side authorisation. URL organisation context is authoritative; submitted organisation IDs are ignored. Every Finance query/write is scoped, with same-organisation FK checks on linked records. An organisation row lock serialises financial changes; version checks reject stale edits, unique request keys reject duplicate captures, and original/reversal/replacement uniqueness prevents duplicate corrections.

Member templates receive curated purpose/party fields, not internal descriptions, internal payer/payee details or correction reasons. Only approved financial records enter their totals. A visible notice explains that unpublished records are excluded, so this may not be the complete financial position. A commitment and its payments/reversals publish or unpublish together. Budget categories and approved amounts are visible; internal revision explanations remain manager-only.

Supporting documents retain their existing controlled-download route and access rules. Finance never changes document classification. Member Finance exposes supporting downloads only for MEMBERS documents that the current actor may access. Restricted document names/links, provider contacts and confidential decision descriptions are not copied into transparency views.

## Calculations

- Period income and expenditure: signed posted entries and reversals dated within the selected March-February year, through the earlier of today or year-end.
- Opening recorded funds: all earlier income minus expenditure plus adjustments.
- Recorded funds: opening funds + period income - period expenditure + signed adjustments.
- Outstanding commitment: effective approved amount minus linked net expenditure; cancelled commitments release the unpaid remainder.
- Available funds: recorded funds - outstanding commitments.
- Approved budget: latest effective revision of each category for the selected financial year.
- Budget remaining: approved budget - period expenditure - outstanding commitments.

Unpaid commitments carry forward and reserve the selected year's available budget. Effective-dated commitment revisions and budget revisions preserve historical year-end views. Reversals retain the original classification with an opposite signed amount, so they undo both cash and category actuals correctly. Reversing a payment reopens its uncancelled commitment. Negative availability or budget remaining is shown as a shortfall, not hidden.

## Integration and audit

Transactions/commitments link to existing providers, work orders, decisions and controlled documents. Work-order/provider consistency is checked. Payments inherit commitment links/category/publication group and cannot exceed its unpaid balance. Finance does not change work-order, governance or document lifecycle states, dispatch work, complete work or confirm bank transfers.

Posted mistakes require a separate reversal with reason. An optional replacement links to the reversed original. Budget revisions and commitment revisions remain reviewable by managers.

Every material action writes `UipAuditEvent` with actor, organisation, entity/reference and safe change information under `finance.*`: transaction.created, transaction.corrected, transaction.reversed, commitment.created, commitment.changed, commitment.cancelled, budget.created, budget.revised and visibility.changed. It never writes CoreAuditEvent. Because existing UIP audit readers can include owners, general audit metadata records changed field names/states/revision pointers rather than private financial values or reasons; detailed values/reasons remain in the Finance histories.

## UI

Finance sits before Reports as the eighth section. Native details/summary groups open the active section by default, retain current-child highlighting, allow keyboard toggling and nest inside the existing mobile Navigation menu. Existing link permissions/URLs are unchanged. Detail/setup/provider-performance aliases expand the appropriate group.

Reusable blue, green, amber, rose, teal and violet pastel stat variants cover Command Centre, committee summary and Finance. Labels/values stay separate; link underlines, pointer, focus outline and subtle hover lift communicate clickability without relying only on colour. Standard cards/forms stay neutral. Finance forms use native UIP controls; budgets revise by named category with hidden version checks.

## Validation

- All 46 UIP HTML templates parsed.
- Consolidated focused run: **229 passed** across test_finance, test_redesign, test_completion, test_work_orders, test_work_order_access, test_providers and test_phase49.
- Final targeted rendering/acceptance/private-document checks after the UI follow-up: **3 passed**.
- Finance coverage includes tenant IDOR, forged links and organisation IDs, all existing roles, CSRF, Decimal input validation, duplicate/stale requests, commitments/partial payment/cancellation/reversal, budget revisions, March/leap-year boundaries, published-only totals, document privacy, CSV safety, DB/model parity and immutable financial history.
- A real overlapping PostgreSQL-session test proves two competing payments cannot overpay one commitment.
- Migration preservation compares every pre-existing synthetic table's rows before/after, excluding site_hit and visit_log; unchanged. Model column/type/nullability, FK, unique and check constraints match the migration.
- Existing datetime.utcnow deprecation warnings remain; no test failures remain.
- **34 viewport checks passed**, with no page or tile overflow. Desktop 1440px and mobile 390px screenshots/checks cover the nine requested representative pages plus Finance overview, member overview, transactions, budget, commitments, capture and detail screens. Active group, keyboard nesting, focus, palette distinction and page/tile overflow are checked.

## Acceptance journey

The focused acceptance test uses real routes/CSRF and existing UIP services in a disposable schema. It creates an annual budget, records R100,000 income and a linked R15,000 provider commitment, and asserts recorded funds R100,000 / commitments R15,000 / available R85,000. The provider legitimately completes the work; no payment is inferred. The manager records R15,000 expenditure against the commitment. The commitment becomes PAID, expenditure R15,000, outstanding R0 and available funds remain R85,000; the work order remains COMPLETED rather than being automatically closed. The resident views the purpose, amount and permitted supporting document, cannot write, and the provider cannot browse Finance. A deliberately incorrect transaction is reversed without deleting its original. UIP audit actions are asserted. All test schemas are removed by fixture cleanup; file fixtures/screenshots remain only in temporary validation directories.

## Limits and operational handoff

This is recorded ZAR financial information, not bank reconciliation, payment execution, an annual-financial-statements replacement or BudgetCash. There is no bank/Paystack/Yoco integration. Existing manager roles provide financial administration; no dedicated finance-officer role is introduced. Each record currently links one controlled supporting document; future statement/reconciliation records can reference the immutable ledger IDs without changing posted history. Posted narrative/link errors require reversal/replacement; commitment amount/cancellation changes use revisions. Large-register pagination is not included in this first phase.

No remaining local validation blocker was encountered. Production migration and deployment remain deliberately unperformed. Other AIT products, payment/enrolment tables, legacy migrations and operational lifecycle services were not modified. Scratch preview files were not modified.

## Validation artifacts

Temporary directory: `C:/Users/Sanjith/AppData/Local/Temp/uip-phase10-validation/`.

- `final-focused-tests.txt`
- `final-ui-and-acceptance.txt`
- `inspection.json`
- `overview-1440.png`, `overview-390.png`
- Individual `<page>-1440.png` and `<page>-390.png` files.
- `finance-navigation-390.png`, `finance-budget-revision-390.png`.

## Files

Changed existing files:
- `app/models/uip.py`
- `app/uip/__init__.py`
- `app/uip/completion_routes.py`
- `templates/uip/dashboards/committee.html`
- `templates/uip/dashboards/manager.html`
- `templates/uip/navigation.html`
- `templates/uip/ui.css`
- `tests/uip/conftest.py`
- `tests/uip/test_redesign.py`

Created files:
- `app/models/uip_finance.py`
- `app/uip/finance_routes.py`
- `app/uip/services/finance.py`
- `migrations/versions/uip_p10_finance.py`
- `templates/uip/finance/_budget.html`
- `templates/uip/finance/_budget_form.html`
- `templates/uip/finance/_fields.html`
- `templates/uip/finance/_transactions.html`
- `templates/uip/finance/_year.html`
- `templates/uip/finance/budget.html`
- `templates/uip/finance/commitments.html`
- `templates/uip/finance/detail.html`
- `templates/uip/finance/form.html`
- `templates/uip/finance/overview.html`
- `templates/uip/finance/transactions.html`
- `tests/uip/test_finance.py`
- `docs/UIP_PHASE10_FINANCE.md`
