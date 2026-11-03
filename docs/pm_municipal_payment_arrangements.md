# Stage 2: Municipal Payment Arrangements

## Existing foundation inspected

`BilProperty.metro_arrangement_amount/duration` and `BilMuniAccount.arrangement_date`, `ca_contract_number`, `ca_agreement_amount`, `ca_installment_amount`, `ca_amount_owing` and related fields are legacy summaries. They use Float amounts and lack formal instalment/payment histories. No complete arrangement workflow or allocation register was found. Lease utility arrangements concern lease allocation wording, not municipal repayment contracts, and were not reused or modified.

`bil_muni_payment` already records account-level municipal payments, using a Float amount. Read-only local PostgreSQL inspection confirmed its columns (id/account_id/month/payment_date/amount/reference), account property_id and existing legacy arrangement columns. Production schema was not inspected. Tenant payments/ledger and statement charges are separate authorities and remain untouched.

## Reused infrastructure and new records

Authenticated, property-scoped routes:

* `/billing/utilities/<property_id>/payment-arrangements`
* `/billing/utilities/<property_id>/payment-arrangements/<arrangement_id>`

Reuse existing Utilities navigation, property manager scope, municipal account IDs, global CSRF protection, tile/flash styling and PM template loader. Active authenticated users may manage arrangements within their authorized property portfolio. The existing target-property authorization applies; no ApprovedAdmin lookup, role label or additional grant is required. This applies to navigation, direct routes, and every arrangement write, and grants no cross-property access.

Three additive tables are necessary: `bil_municipal_arrangement` (formal agreement terms and maintainable status/document reference), `bil_municipal_arrangement_payment` (decimal-safe arrangement allocations/evidence) and `bil_municipal_arrangement_event` (actor, timestamp, before/after data and persistent request receipts). This is an arrangement monitoring register, not a second financial ledger. No legacy amount is converted, backfilled, inferred or overwritten.

Amounts use Numeric(18,2)/Decimal. Schedules are derived from saved formal terms; no separate schedule table is needed. Weekly, fortnightly, monthly and quarterly frequencies are supported, up to 600 instalments. Monthly/quarterly dates retain the original day, clamped to each month-end. The final instalment may be smaller; all instalments sum exactly to the original amount. Instalments are allocated oldest first. Overdue means due before today with an unpaid amount. Fully allocated arrangements display Completed. Entry corrections use version-checked Edit with before/after audit history. A distinct renegotiated agreement can be recorded separately to preserve its agreement identity.

Administrators can record external payment evidence or link an existing account-level municipal payment in full. Linking reads its saved date, exact cent amount and reference; source payments with unsupported precision are rejected rather than rounded. Each source payment can be allocated only once. A payment reference is unique within its municipal account. Payment dates cannot precede the agreement or be in the future. Overpayment is rejected. Partial splitting of an existing municipal payment is outside this initial workflow; partial manual instalment payments are supported. Supporting documentation is a reference field, not a new upload/storage facility.

Persistent UUID request receipts and hashes prevent duplicate submissions or reuse with altered data. Row locks serialise allocations and outstanding-balance checks. Status edits use optimistic version protection. Transactions commit terms/allocation/audit together. All writes are POST-only and protected by CSRF and account/property authorization. Missing migration disables the workflow safely.

## Financial safeguards

This feature never writes `bil_muni_payment`, tenant ledger entries, municipal bill balances, tariff rates, METSOA caches, issued statements, payments/credits in other systems or consumption. Existing payment links store a source snapshot in the audit event. Manual payment evidence does not claim to have posted or reconciled a payment elsewhere. Outstanding arrangement balance is derived from the agreed amount minus arrangement allocations only.

No Setup Wizard, Tariff Management, METSOA formula/generation, Tenant SOA generation, Metro Reconciliation, Lease, Ledger or payment-processing code changes. No production data or migration execution is part of delivery.

## Migration and deployment

Forward-only revision `pm_muni_arrangements_001`, parent `pm_tariff_authority_002`. Adds the three constrained tables with RESTRICT foreign keys; does not modify or migrate legacy financial rows.

After explicit approval and the established production database identity checks, the Render web service Shell command is:

    python -m alembic upgrade pm_muni_arrangements_001

Do not run this command automatically. Confirm parent revisions and required existing tables (`bil_property`, `bil_muni_account`, `bil_muni_payment`, `user`) before applying. Verify the revision and three empty new tables after migration, then authenticate as an administrator with property access and open Utilities > Payment Arrangements. Do not create live test payments. Production deployment and authenticated live verification are separate from successful local tests.

## Tests

Real Alembic upgrade on session-temporary PostgreSQL tables, register/detail routes, schedule/date arithmetic, month-end/leap-year handling, decimal/amount validation, allocation/balance/overdue calculations, duplicate references and submission retries, existing payment linking, source/property isolation, administrator checks, CSRF, status/history/stale edits and financial sentinel preservation. A browser creates an arrangement, records payment evidence, refreshes safely, updates status and navigates back with property/month preserved. Independent PostgreSQL sessions verify concurrent overpayment prevention and same-request replay safety using randomly named disposable local tables.

## Production navigation correction (3 November 2026)

Render shell inspection verified deployed commit da0608ee and used a standalone
read-only PostgreSQL transaction without importing the application. Bill (644)
is active, has role user, manages Almond (26), and has no active ApprovedAdmin
grant. The real Utilities route renders program_billing/utilities_hub.html and
includes municipal_navigation.html; its can_manage_municipal_arrangements()
condition was correctly false under the previous administrator-only policy.
The cause was authorization policy, not a missing template or navigation context.

The corrected policy uses the existing authorized-property portfolio for all
active authenticated users, without any ApprovedAdmin dependency. No grant
or business record was changed. Regression coverage uses Bill's access state,
no admin registry, blueprint-scoped template context, and the real
Utilities template; it verifies the existing seventh link, arrangement/payment
audit actor, financial preservation, inactive denial, and cross-property denial.

## Simplified arrangement entry

Create no longer asks for First Payment Date, Payment Frequency, or Supporting
Document Reference. Existing database columns and historical values remain.
For omitted schedule terms on new records, the retained NOT NULL columns use
the agreement date and monthly frequency; the schedule/allocation arithmetic is
unchanged. Existing explicit terms remain valid for compatibility. Status/note
updates preserve the saved document reference rather than clearing it when the
removed field is absent. The detail summary omits frequency and first/final
payment dates, and the status form no longer edits document references. Account
selection, amounts, payment evidence and append-only audit history are retained.

## Scheduled register balance

The register columns are Account, Arrangement, Agreement Date, Debt, Instalment,
Balance, Action. Balance is max(0, original agreement amount minus monthly
instalment times elapsed calendar months inclusive of the agreement month and
selected billing month). Before the agreement month the full debt is shown.
This projection uses agreement_date, original_amount and instalment_amount;
it never treats scheduled instalments as actual payments or changes records.
Actual payment evidence, audit history and underlying management routes are
preserved separately. The register exposes only the agreement-correction Edit action, without payment management.
The Number of Instalments input is removed. For new records its retained
database column is derived as ceiling(debt / instalment), including a smaller
final instalment. Existing column values and historical terms are unchanged;
the existing 600-period storage constraint remains enforced against the amounts.

## Audited agreement correction

The register's only action is Edit, opening an agreement-only form. It can
correct reference, agreement date, debt and monthly instalment. Account/property
association is immutable; it cannot be reassigned through correction.
A row lock and version token protect concurrent edits; UUID receipts retain
retry safety. Changes append a corrected event containing before/after terms
and actor. Existing audit rows, payment evidence and source financial tables
are unchanged. Debt cannot be reduced below saved payment evidence and an
agreement cannot be dated after an existing payment. Retained schedule count
is rederived from the amounts; any required schedule-anchor adjustment is
included in the before/after audit. The register balance is still a read-only
monthly projection, independent of actual payment evidence. The Edit form
contains no payment-management or status form. Existing management routes
remain available independently.
