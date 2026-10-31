# Property-based utility capture

Revision pm_utilities_001 follows metsoa_setup_002. No production migration or data change was performed during development.

## Existing sources and boundaries

Municipal Accounts uses current property-owner registrations in bil_property_ownership / bil_owner_identity. A legacy saved municipal owner remains visible and can be retained unchanged, but new owner choices must be registered against the selected property. Municipal ownership continues using the existing ref_muni_owner reference; no historical owner data is renamed.

Meter Readings uses bil_meter_reading, existing meter IDs and property_id. First readings create a baseline only. Subsequent readings reuse utilities_readings.save to calculate consumption. No account, utility-type or sectional-unit selection is needed. Edits retain an audit snapshot; readings supporting consumption or later readings are blocked from ordinary editing. Resolving those requires a separately reviewed historical correction, not an overwrite.

Metro Reconciliation uses a new bil_municipal_statement table because the existing bil_muni_cycle_totals fields are floating point and have no statement/due dates. Original float totals are not migrated or recalculated. Statement capture uses Decimal and NUMERIC(18,2), a unique account/month key, optimistic version checks and a shared property-row lock. Edits append before/after snapshots to bil_utilities_capture_audit. No financial transactions are created.

Paid is read-only from bil_muni_payment.account_id + month, the existing municipal payment allocation. Existing code writes payments with those explicit keys. Other accounts and months, tenant ledger, METSOA due totals and property-wide payments are never used. If that allocation schema is absent, Paid is Unavailable. Historical payments recorded only in other ledgers need verified municipal-account/month allocation before they can be included. No new payment posting or allocation is implemented. The production payment catalog and actual allocations were not independently inspected.

## Deployment / migration (explicit approval required)

Deploy the committed code through existing Git/Render workflow. The new capture pages block saving safely until migration is applied. Owner dropdowns require existing property-owner registration tables; an empty dropdown means register the property owner through the existing Property & Owner workflow.

After explicit approval, take the normal database backup. In the existing authenticated Render Shell at the deployed repository root, using the existing DATABASE_URL:

    python -m alembic -c alembic.ini current
    python -m alembic -c alembic.ini upgrade pm_utilities_001
    python -m alembic -c alembic.ini current

Use this named revision, never head/heads, Flask startup, stamps or manual production DDL. The migration creates only bil_municipal_statement and bil_utilities_capture_audit; no existing financial, meter, reading or setup rows are changed. It requires existing metsoa_setup_002 prerequisites. Unexpected existing tables or missing keys abort rather than replace records. Lock wait is bounded to ten seconds. Downgrade is blocked to preserve capture history.

Verify pm_utilities_001 is current, NUMERIC(18,2) statement amounts, the unique account/period constraint and RESTRICT account/property foreign keys. Compare pre/post existing account, meter, reading, consumption and payment counts/values. No historical statement backfill is automatic. Old float monthly totals remain separate historical source material; any reconciliation requires review. Do not enter production test transactions.

## Tests

Focused isolated PostgreSQL tests execute the real migration in temporary tables; cover current owner registrations, property-scoped dropdowns, canonical baselines, reading creation/edit audit and history blocks, existing consumption integration, Decimal statement creation/edit, unique monthly records, retry protection, payment account/period attribution, schema safeguards, permissions and CSRF. Existing Municipal Accounts and Setup Wizard protection tests remain in the regression suite. No live data or signed lease documents are changed.
