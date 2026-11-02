# Municipal tariff authority and statement-evidence stop gate

## Completed Stage 1

Only authenticated platform administrators can create or version municipal tariffs through the existing Tariff Management routes. The shared posting functions also recheck authority. Municipality, municipal tariff category and an official source document/bill reference are mandatory for every new version. The saving administrator is the verifier; the database supplies verification and recording timestamps. Browser-supplied actor/timestamp values are ignored. A tariff row and its evidence insert commit atomically; an evidence error rolls back the new tariff too.

Automatic code allocation and existing append-only future rate versioning remain unchanged. Existing rates/factors/dates are not rewritten. An evidence record is never fabricated for legacy tariffs. The list and history identify unverified existing versions as Legacy. If the migration is absent, existing tariffs remain readable, verification availability is explained, and all saving fails safely.

## Migration (NOT applied to production)

Revision: pm_tariff_authority_001
Parent: pm_meter_exceptions_004

Adds only bil_tariff_verification, with a one-to-one tariff foreign key, municipality, category, source reference, legal effective date, verifier user foreign key and timezone-aware server timestamps. Nonempty sources are constrained in PostgreSQL. No existing table rows are updated, no rates are changed, no backfill is attempted and no legacy record is marked verified. The migration is forward-only.

After explicit approval, use the production web service Render Shell, with its assigned DATABASE_URL and normal migration identity checks:

    python -m alembic upgrade pm_tariff_authority_001

Before running, confirm current_database() is ait_platform_db and the authorised service/database identity. Do not change credentials or local .env. Verify the revision in alembic_version, the new table/constraints and required columns. Immediately after migration, before any genuine new tariff saves, the verification table should be empty; existing rates must have no fabricated evidence. Deployment alone does not apply this migration.

## Safety audit and mandatory stop

The requested Stage 3 says to STOP before enabling corrections if original issued statement values cannot be reliably identified. This condition remains unresolved. Consequently backdated saves, recalculation, revision issuance, adjustment recovery and emails have NOT been enabled.

Evidence inspected:

* app/models/billing.py::BilStatement defines only id and tenant_id. The prior read-only local PostgreSQL inspection confirmed those columns; production artifacts have not been inspected.
* app/program_billing/lease_soa_statement.py::statement projects the current ledger. It does not freeze an issued statement's tariff versions, consumption/tier lines, tax/rounding rules or payments/balances at issuance.
* app/utils/billing_persist.py::_upsert_metsoa_row updates monthly results, and _replace_breakdown deletes/replaces detail lines. These caches cannot establish an immutable originally issued amount. These functions were not invoked or modified.
* app/utils/tariff_versions.py::resolve deliberately rejects duplicate identity/effective-start records. It has no supersession/as-recorded mechanism for corrections sharing a legal start date.
* The existing Utilities Output signed preview is transient and is not an immutable issued Tenant SOA/METSOA financial snapshot.

## Required evidence design before continuation

First locate and verify any already issued database values or original emailed statements. Missing originals must be labelled Cannot Calculate - Review Required; do not backfill guessed readings, tariffs, taxes, dates or balances.

An additive database-only issuance design must preserve immutable Original financial snapshots: statement identity/type, tenant/property/month, original meter readings and consumption, tariff IDs/rates, tier breakdowns and factors, fixed/surcharge/tax components, exact rounding rules, charge totals, payment/credit recognition and ledger source links, balances, issue timestamp and original content checksum. No new permanent R2 PDF storage is proposed.

Revised V1/V2 snapshots must link to their predecessor and tariff correction, retaining separate legal effective and recorded timestamps, actor and supersession lineage. Recalculation can use existing formulas only once their original inputs are established. Revision approval and delivery attempts need unique idempotency keys; saving a tariff must not issue/email a statement.

Adjustments compare revised charges with the latest recognised charges, never running balances. Preserve every payment/credit; do not recreate old debt. Municipal changes pass through subject to verified lease/billing authority. If recovery is not legally established, hold for review. There is no owner-subsidy option. Any approved posting must use the existing PM ledger and recognise only the outstanding difference once.

The original-statements backup warning is useful but cannot replace immutable database evidence. No backup confirmation, revised-statement button, adjustment posting or automatic email is introduced before this safety gate is resolved.

Setup Wizard, calculation formulas, historical financial records, Lease, Ledger, Meter Exceptions, Bulk Meter Linking, Metro Reconciliation and payment processing are unchanged. Production migration, production data changes and authenticated live verification require separate approval/access.
