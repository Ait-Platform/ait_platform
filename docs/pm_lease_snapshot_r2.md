# Lease snapshot and private upload foundation

The workspace catalog contains 60 items transcribed from the hash-verified R01 DOCX: Core Lease, Schedule 1, Special Conditions and Annexures A-H. The source uses bracketed placeholders, not Word MERGEFIELD instructions. Classification describes workflow data completeness only; it does not certify legal approval, signature readiness or activation. Not Applicable is available only for conditional items and requires an explanation.

## Persistence requiring review

`docs/proposed_migrations/pm_lease_001.py` is deliberately outside Alembic discovery. It proposes two additive tables: `bil_lease_draft` (property/manager, idempotency token, source version, JSONB particulars, optimistic version, draft-only status and timestamps) and `bil_lease_document` (scoped draft/property/manager, item, private object key, filename, MIME, size, SHA256, pending/available/failed state and verification timestamps). Foreign keys restrict deletion; existing property and lease tables are untouched. No normal application model creates these tables automatically.

Before integration: inspect authoritative production schema and Alembic heads using the read-only SQL, approve branch ancestry, obtain and verify a backup, rehearse on a restored database, then obtain explicit migration approval. Never stamp a revision without verified schema correspondence. Production migration execution has not occurred. Downgrade deliberately refuses destructive deletion; use an independently approved recovery procedure.

`PM_LEASE_DRAFT_WRITES_APPROVED` defaults false. Writes additionally require both tables, required columns and the applied `pm_lease_001` revision. Draft data is PostgreSQL-backed, never cookie-backed. Authorization checks both the current property manager and draft manager. Writes lock the property and draft, and updates use optimistic versions.

## Storage destination and privacy gate

Only the existing general `R2_BUCKET_NAME` is used. HOME uses `SACE_HOME_R2_BUCKET`; equality is rejected. Keys are `private/leases/property-{id}/draft-{uuid}/document-{uuid}.pdf` or `.docx`. The existing authenticated immutable upload/read-back helper is reused. No public or presigned object URL is returned.

Repository instructions document the general Reading bucket as `ait-platform-assets`, with public Reading delivery. This is not verification of Render configuration. A private-looking prefix does not make a publicly accessible bucket private. Before enabling `PM_LEASE_PRIVATE_STORAGE_APPROVED` (default false), independently verify that every public access path, including r2.dev, denies lease objects while preserving Reading. If that cannot be achieved safely with the current infrastructure, uploads must remain disabled; no new bucket or policy change is included here.

Uploads validate PDF/DOCX structure and a 10 MB limit. Metadata is committed as pending before immutable upload and read-back verification. Only verified uploads become available after reauthorization. Cross-resource failures retain metadata for reconciliation; no unsafe compensating object deletion occurs. An administrator must inspect pending records and object existence/hash before retrying or cleanup. Downloads reauthorize property access and verify hash/length through the server.

## Verified locally and not verified remotely

The original local R01 pair in `output/pm_stage5` matches the fixed application hashes. The default private document root remains the application instance directory `/pm_lease_documents`, overridden by `PM_LEASE_DOCUMENT_ROOT`, with the R01 subdirectory. Render root/files and Almond property 26 have not been verified; no association was inferred or changed.

The current process does not provide authoritative general/HOME bucket names, R2 credentials or a production document root. `scripts/pm_lease_storage_preflight.py` reports configuration presence and local file hashes without credentials; it does not establish Cloudflare privacy or production identity. No real Render upload/download roundtrip has been performed. Tests use disposable PostgreSQL tables and a fake authenticated R2 client, including read-back and failure paths.

Dedicated item pages and attachment pages preserve the workspace navigation without repeating the horizontal menu. Existing R01 viewer/download routes remain. Review/merged document generation, legal approval, activation and signature workflows remain future work and are not represented as operational.
