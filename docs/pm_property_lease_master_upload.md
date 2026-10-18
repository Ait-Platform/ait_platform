# Property-specific Lease master uploads ? local review

## Scope and entry

Select the existing property in Lease, then choose **8. Upload Master Document**. The existing property ID is used. A lease draft is not required or created. If a draft ID is supplied, it must pass existing authorization checks and is preserved on return. The upload page shows the property name, highest verified available version, all prior uploads and authorized attachment-download links.

Uploaded files do not select or replace the pinned Stage 5 template. View Lease, placeholder mappings, saved particulars and Generate Word remain unchanged.

## Migration

`migrations/versions/pm_lease_master_001.py` is a new independent `pm_lease_master` Alembic root (`down_revision=None`, `depends_on=None`). The existing application requires the exact `pm_lease_001` revision row; making this a child would remove that marker when advancing the branch. The new table references only existing `bil_property` and `user` IDs. No existing migration is rewritten, and no business tables/records are altered.

The only table is `bil_property_lease_master`:

- UUID `id` primary key; integer `property_id` with restrictive FK to `bil_property.id`.
- Positive integer `version`, unique per property (`uq_pm_property_master_version`).
- `original_filename` varchar(200), `mime_type` varchar(100), unique `storage_key` varchar(500).
- `size_bytes` integer (1?10485760), lowercase 64-character SHA-256.
- `uploaded_by` integer with restrictive FK to `user.id`; timezone-aware `uploaded_at`, default now().
- `state` varchar(16), default pending: pending/available/failed.
- Nullable timezone-aware `verified_at`; required only when available and forbidden for pending/failed.
- `ix_pm_property_master_current` on (property_id,state,version), plus primary/unique indexes.

The migration fails on existing table/object conflicts rather than silently reusing or overwriting objects. Transactional DDL rollback is tested locally. Destructive downgrade is disabled; operational rollback preserves the table/files and removes the feature deployment instead of deleting historical uploads.

## Version allocation and failure handling

Each upload rechecks manager authorization and locks the existing property row (`FOR UPDATE`), selects MAX(version)+1 over all states, and inserts pending metadata. PostgreSQL also enforces property/version uniqueness. Pending metadata is committed before R2 I/O so network calls do not hold the property lock. Different uploads receive UUID object keys, preserving old files.

The immutable Lease R2 upload API writes bytes and reads them back. A second scoped read verifies size, SHA-256 and byte equality. The manager/property lock is checked again after network I/O before changing pending to available and committing. If authorization changes or verification fails, the version is failed and never current. A process/database interruption can leave pending metadata and an R2 object; pending records remain non-current and non-downloadable. Recovery must inspect those records rather than deleting previous versions or claiming success.

Current means the highest **available and verified** version, not the last upload attempt. Failed attempts leave version gaps. There is no deletion/overwrite endpoint.

## Validation and storage

DOCX only, 10 MiB maximum. Validate ZIP package bounds, required content types/document XML, path traversal, duplicate entries, encryption/macros, XML/DTD/entity declarations and CRC. Original filenames are retained as display/download metadata only; keys use property ID plus UUID:

`private/leases/property-<property_id>/masters/master-<uuid>.docx`

The Lease adapter uses its dedicated authenticated R2 transport and the `ait-lease-controlled-private` bucket by default. It never inherits public-platform/HOME credentials or returns storage keys/public URLs to users. The platform immutable conditional-PUT helper is reused without changing its shared client. Downloads verify the requested property, current manager, available state, expected key scope, byte length and digest. Responses are private/no-store with nosniff; downloads are attachments.

Lease-only environment settings:

- `PM_LEASE_R2_BUCKET=ait-lease-controlled-private` (also the application default).
- `PM_LEASE_R2_ENDPOINT_URL` ? HTTPS account S3 endpoint, without embedded credentials.
- `PM_LEASE_R2_ACCESS_KEY` and `PM_LEASE_R2_SECRET_KEY` ? dedicated restricted credentials for the Lease bucket.
- `PM_LEASE_PRIVATE_STORAGE_APPROVED=false` remains the default and must remain disabled pending independent verification.

No fallback to `R2_ENDPOINT_URL`, `R2_ACCESS_KEY`, `R2_SECRET_KEY` or HOME credentials occurs. HOME/public platform configuration is unchanged. Explicit attempts to select their configured bucket names fail closed. Requests use TLS verification, short connection/read timeouts and conditional immutable writes with read-back integrity checks.

Before approval: independently confirm this bucket exists, public development URLs and unauthorised custom-domain/Worker access are disabled or blocked, and authenticated retrieval succeeds for a controlled non-sensitive object. Verify those outside the application's disabled approval gate using separately authorised administrative inspection. Local R2 doubles do not establish actual privacy or network retrieval. No live verification, environment changes or storage activation occurred.

Existing Lease objects, if any, are not automatically moved from the former general bucket. Existing metadata/key references are preserved. Any needed copy/verification requires a separate reviewed operation before activation; there is deliberately no insecure fallback to the old bucket.


## Local evidence and production boundary

Migration applied through Alembic Operations only in disposable loopback PostgreSQL `ait_local_db` fixtures and separate isolated schemas. No application factory/startup maintenance or production database was used. R2 is a controlled API double: these tests prove flow, scope/integrity enforcement and failure handling, **not a live R2/Render upload**. Two concurrent real PostgreSQL connections exercise version allocation. Existing revision rows, property records and draft JSONB are checked for preservation.

Before production: review implementation/tests, verify actual schema/object collisions and CREATE/REFERENCES privileges for the migration role, obtain/restore-test a current backup, rehearse the targeted migration on that restored copy, and obtain explicit approval. Application role needs SELECT/INSERT/UPDATE on the new table and existing property lock privileges; no grants are executed automatically. Run a targeted upgrade of `pm_lease_master_001` using the reviewed project-root Alembic configuration, never `upgrade heads`, startup migration or blind stamping. Preserve every existing Alembic revision row. Verify definitions/constraints and private R2 read/write integrity before a real authorized test upload. No production migration, deployment, commit or push is authorized by this local review.
