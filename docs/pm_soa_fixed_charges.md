# PM SOA Fixed Charges and display delivery

## Display stage

Tenant Ledger overrides only its own parent flash block and renders the existing shared flash partial inside its tile. Other pages and shared flash styling are unchanged.

Transactions show a display description and separate reference. Technical reversals display as Reversal. Audit actor IDs, UTC recorded timestamps, correction reasons, original/replacement explanations and stored posting notes are hidden from the transaction table, not deleted.

txn_date is the effective transaction date. Existing naive timestamp values represent saved calendar dates and are not silently reinterpreted as UTC. Timezone-aware effective timestamps are displayed in Africa/Johannesburg. New-form defaults use the South African date. correction_recorded_at remains the unmodified UTC-aware audit timestamp. No production record was inspected or rewritten to guess a different historical date.

The initial window contains the latest five transactions, in chronological order. Show More expands the window backwards by five; Show Less restores the latest five. Running balances and outstanding balance are calculated over the complete ledger before slicing.

## Existing infrastructure and reuse

The actual local PostgreSQL bil_tenant_recurring was inspected read-only. Its columns are id (sequence default), tenant_id, description varchar(255), kind varchar(50), amount numeric(10,2), day_of_month integer and is_active integer. Existing legacy materializers expect missing month fields and can post through legacy paths. Existing cron_run_billing.py targets shared Debtor accounting. None is called or modified.

PM rent definitions reuse bil_tenant_recurring, with additive PM scope, date, status, version and idempotency fields. There is one PM rent definition per tenant/property, editable through Fixed Charges. Existing legacy definitions are not converted or edited. PM definitions keep legacy is_active=0 and use pm_active for the visible status. A check constraint prevents accidental legacy activation, avoiding a second materializer for these rows.

All rent is posted through the existing PM posting service's shared append_entry primitive, using Decimal rand, the same D reference sequence, ledger model, persistent posting keys, managed-property and registered-tenant association checks. A worker supplies only the manager scope saved in the schedule and rechecks it against the actual property and tenant. No authenticated user is impersonated and no HTTP permission can be supplied from a form.

## Schedule semantics

- Description is fixed to Rent Due. Amount is positive rand with at most two decimals.
- Start/end dates are inclusive. Monthly day defaults to 1; values 1..31 clamp to the last day in a shorter month, including leap years.
- If the month's scheduled day precedes the start date, that month is skipped; no prorated or early charge is invented.
- No date after today in South Africa is posted. No due date beyond the end date is posted.
- Missed due months are caught up, including due dates within an expired schedule when the worker resumes later. Already posted months are skipped efficiently.
- Inactive pauses posting. Reactivation catches unposted due dates within the configured interval. Configuration changes affect unposted periods; posted amounts and references remain untouched.
- Schedule-row locks and a unique tenant/property/month ledger index prevent duplicate rent under concurrent runs or configuration changes. Corrections retain the original monthly marker, so correcting rent does not cause another scheduled charge.
- Catch-up is bounded to 120 new months per schedule per run; subsequent runs continue safely.
- Existing four-digit D reference capacity applies. Exhaustion fails the affected schedule without wrapping references or falsely reporting success.

## Migration — not applied in production

pm_soa_003 (child of pm_soa_002) adds PM columns/indexes/checks to the existing recurring table, two nullable rent-link columns to the existing ledger, a same-tenant/property schedule FK and a unique monthly-rent index. No new ledger or financial history rewrite is introduced.

Migration was exercised only on isolated local PostgreSQL fixtures and through the actual Alembic environment. Follow the existing backup, restored-copy rehearsal, independent Render database identity and approval procedure before any production upgrade. The approval target is pm_soa_003; any absent pm_soa_001/002 ancestors would also be applied and must be included in that approval. Do not stamp, manually add columns, upgrade unrelated heads or bypass schema guards.

## PM-only worker — not enabled in production

The implementation does not start a timer inside Gunicorn and does not post from GET requests or application startup. An external daily PM-only job must be configured and enabled after explicit approval, compatible production migrations and review of active schedules.

From the service repository root, use the same approved code checkout and assigned DATABASE_URL / existing secret environment group:

```bash
python scripts/pm_soa_post_fixed_rent.py --run
```

Suggested UTC cron expression: 5 22 * * * (00:05 South African time each day). A more frequent schedule is also idempotent. Do not repoint or replace the existing shared Debtor/ProShop billing cron.

Without --run the script prints a no-write notice and makes no database connection. With --run it builds only a minimal worker Flask context; it never calls create_app() or unrelated application startup maintenance. It exits nonzero on missing schema or any failed schedule and outputs only posted/failed counts. Logs identify the schedule and exception category without echoing connection strings.

No Render cron was created/enabled, production migration applied, or live financial transaction created during implementation. Automatic production execution remains pending those operational steps. Isolated tests exercise the real worker entry point, month-end/leap-day rules, catch-up, no future posting, inactive schedules, stale/duplicate forms, ownership denial, concurrent monthly uniqueness and rollback/retry.

## Fixed Charges GET availability repair

The GET route previously called the write-schema guard unconditionally and propagated its explicit HTTP 503 before rendering. This is reproduced with an absent pm_soa_003 monthly-rent index on isolated PostgreSQL. The page now returns HTTP 200 with the exact migration notice inside the tile and no enabled schedule form. Save/worker schema checks remain fail-closed. No migration or existing financial record is changed by this UI repair. Owner-supplied Render access logs confirm the Fixed Charges GET returned 503 while ledger/correction requests succeeded. They contain no exception or response body; the exact missing production object is not independently confirmed.

## Separate charge tables and fixed utilities

Fixed Charges now opens with separate Monthly Rent and Utility Charges tables. Add Charge and Edit use the existing authenticated, CSRF-protected, versioned save route. There is one configurable schedule per tenant/property/category (Rent Due, Water, Electricity, Sewerage, Refuse Removal or Other Utilities). Existing categories are edited rather than duplicated. Utility setup explicitly records that the monthly amount is agreed and fixed; variable charges are not accepted for automation.

Additional migration pm_soa_004 is a child of preserved pm_soa_003. It adds category/agreement metadata and category-specific monthly uniqueness, without backfilling or rewriting existing schedules or transactions. Existing null categories mean rent, preserving old rent markers. It is not applied to production. A database with only pm_soa_003 can still manage rent; utilities remain disabled with an explicit pm_soa_004 notice.

## Shared rent and utility posting worker

The existing PM worker now processes Rent Due and the five agreed fixed utility categories through the same posting service and D sequence. Monthly uniqueness includes tenant, property, category and billing month. Old null category markers remain rent; no historical reference or amount is rewritten. The worker checks the saved agreement itself before any utility posting, in addition to the form and database check.

Activation remains deferred. After explicit approval and migration/rehearsal review through pm_soa_004, configure the existing command python scripts/pm_soa_post_fixed_rent.py --run as a PM-only scheduled job (suggested UTC cron 5 22 * * *, 00:05 South African time). Review active schedules and their start dates before enabling it; legacy/manual transactions without schedule markers are not guessed as equivalent scheduled charges. Keep the assigned DATABASE_URL in the existing secret configuration. Do not repoint cron_run_billing.py or activate a second worker for these same definitions. Concurrent repetitions are safe, but an independently configured PM job is sufficient. No migration or worker was activated in production.

## Final verification and approval-only activation

The completed focused PostgreSQL suite passed 69 tests. It covers the 503-to-notice GET repair with writes still blocked; both charge tables; creation/editing, stale forms and idempotency; all fixed categories and outstanding balances; leap-day/month-end posting; no early charges; deactivation; tenant/property/manager isolation; concurrent same-period utility uniqueness; and preservation of pre-upgrade rent markers and financial rows. The actual Alembic environment was exercised through pm_soa_004 and its no-op repeat on disposable local tables. pm_soa_003 remains unchanged in Git.

The supplied Render logs confirm only the Fixed Charges GET failed while ledger and correction requests succeeded. They are access logs, not a schema exception report. The unhandled GET write-schema guard is reproduced locally; which production prerequisite is missing still requires the new logged notice or an authorised read-only catalog inspection. Do not claim this is independently verified against live schema.

Production execution remains unapproved and was not attempted. Follow the existing identity, backup, restored-copy rehearsal and privilege checks before seeking execution approval. After that approval, the targeted migration from the verified service repository root is:

```bash
PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c search_path=public -c lock_timeout=5000 -c statement_timeout=120000" python -m alembic -c alembic.ini upgrade pm_soa_004
```

This also applies any absent pm_soa_001..003 ancestors; approval must cover that plan. Verify that the ledger branch ends at pm_soa_004, unrelated revision heads and original financial columns are preserved, utility agreement/category columns exist, and the monthly-rent unique index now includes COALESCE(fixed_charge_type,'rent'). Do not stamp or manually add objects.

Then, only after worker activation approval and review of configured active schedules, enable the PM-only command on the existing assigned database secret configuration:

```bash
python scripts/pm_soa_post_fixed_rent.py --run
```

Suggested schedule is UTC cron 5 22 * * * (daily 00:05 South African time). The command handles missed due dates and retries; do not change the shared Debtor/ProShop cron. Monitor posted/failed counts and nonzero exits. No production test transactions are required or authorised by this procedure.
