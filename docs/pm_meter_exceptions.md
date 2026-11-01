# Account-linked Meter Exceptions

The existing /meter_exceptions/<property_id> route is retained. Municipal Accounts links it with account_id; the Utilities menu opens the property-wide register. Account selection is required for registration. Only registered property/account meters are selectable, including account pointers and verified existing account-number associations. Conflicting pointers, cross-property/account meters, utility mismatches, self-replacement, competing replacement parents and circular chains are blocked. No Setup Wizard path is invoked.

Incidents are separate records. Original meter status, theft/replacement fields, identities, readings, consumption and municipal financial records remain unchanged. Existing non-active meter statuses remain visible separately and are never silently converted to incident history. Repeated incidents are allowed; replayed submissions use a persistent UUID key and cannot overwrite prior content.

Continuing charges reference captured bil_municipal_statement records from the incident account. Decimal amounts and a JSON snapshot of the captured bill version are retained. Evidence can be associated with a specific charge. These annotations never deduct balances, post payments or create refunds/credits. A future disputes workflow can reference charge IDs without replacing the source records.

Status updates and notes append events with actor, recorded timestamp, effective date and relevant before/after values. Open, Reported, Resolved and Closed are case statuses, not automatic meter-state changes. Reported requires a notification date. Stale versions are rejected; status/metadata changes roll back if the event insert fails. Prior incidents, evidence and event rows have no edit/delete interface.

## Additive migrations — not applied in production

pm_meter_exceptions_001 follows pm_utilities_001 and creates bil_meter_exception, bil_meter_exception_event and bil_meter_exception_evidence. pm_meter_exceptions_002 follows it and creates bil_meter_exception_charge and bil_meter_exception_charge_evidence. Foreign keys use RESTRICT; enum/date/amount/self-replacement and evidence-size checks apply. Neither migration backfills, deletes or updates existing records. Both deliberately refuse destructive downgrade.

The existing production migration procedure in docs/pm_soa_migration_runbook.md applies: confirm the assigned Render database and public schema, current revision graph, required base keys/types and privileges, absence of conflicting partial objects, verified backup and isolated restored-copy rehearsal; obtain explicit execution approval. Use the root alembic.ini. Do not invoke create_app(), flask db upgrade, upgrade heads, stamping or manual schema repairs.

Proposed targeted command after those checks and explicit approval, from the Render repository root:

    PGOPTIONS="$PGOPTIONS -c search_path=public -c lock_timeout=5000 -c statement_timeout=120000" python -m alembic -c alembic.ini upgrade pm_meter_exceptions_002

The migrations set a bounded local lock timeout of 10 seconds; statement timeout remains bounded by the session above. This command was not executed in production. The revision resolves through the root Alembic graph; upgrade functions were rehearsed only against session-temporary local PostgreSQL tables. Missing/partial exception schema disables registration/charge capture safely while keeping existing account/meter information readable. If an object collision or legacy relationship conflict is found, stop rather than alter existing data.

After authorised migration, verify the targeted revision in public.alembic_version, the five new tables/foreign keys/checks, application-role permissions and unchanged baseline row counts/checksums for municipal accounts, meters, readings, consumption and reconciliation. Verify normal account-scoped GET routes without creating production test transactions.

## Private evidence

PDF, PNG and JPEG content is validated; maximum size is 10 MB. Images must match their declared extension and stay below 30 million pixels; PDFs must be readable and unencrypted. Filenames are sanitised. Evidence is immutable private R2 content with stored size and SHA-256 integrity checks. Downloads require authenticated property/account-scoped access, use opaque evidence IDs, and return attachments with no-store/nosniff headers. No public object URL or storage key is displayed.

The new Utilities evidence wrapper reuses the existing private PM transport in lease_r2 without changing it or any Lease workflow. Existing PM_LEASE_PRIVATE_STORAGE_APPROVED and PM_LEASE_R2_* configuration must be valid; credentials must allow the separate private/pm-utilities/ prefix in the configured private bucket. There is no shared/public-bucket or disk fallback and no new credentials/settings. Live prefix access was not tested here; R2 calls were mocked in isolated tests.

Storage failure rolls back database changes. R2 and PostgreSQL are not one distributed transaction: if storage succeeds but the subsequent database transaction fails, an unreferenced private object can remain. It is not exposed as evidence; any cleanup must be an independently reviewed maintenance task. Existing evidence is never overwritten or deleted by this workflow.

Code deployment, production migration and live storage verification remain separate. No production migration, data write or evidence upload was performed during implementation.
