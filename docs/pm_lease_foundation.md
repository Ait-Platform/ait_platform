# Lease foundation and production diagnostics

## Confirmed code findings; production column not yet verified
The old existing_leases query selects bil_lease.id/start_date/end_date/rent_amount,
joins bil_property ON bil_property.id=bil_lease.property_id, and filters by selected
property ID, current manager and non-archived property. SQLSTATE 42703 means an
undefined column. That code log omitted PostgreSQL's column diagnostic. The supplied
log alone cannot distinguish the five referenced bil_lease columns. No authoritative
production connection is available in this session; naming one as the confirmed live
missing column would be guessing.

BilLease expects these columns, TEXT start/end dates, NUMERIC rent and mandatory
tenant_id. Its tenant relationship is eager-loaded on full ORM objects; the workspace
continues to select scalar fields only. Inspected broad revisions
7da57fffdba9_sync_db_to_models and bec556b95366_add_mech_models alter unrelated
charges/indexes/FKs; they are not a safe targeted repair for an unverified column.
No migration was created or executed.

The workspace now inspects actual lease column names before constructing its scalar
query. Missing optional summary fields are omitted from SQL and explicitly reported,
without guessing aliases. Missing id/property_id disables listing rather than
guessing a property association. SQL failures are contained by a savepoint, preserving
property selection and subsequent owner queries. Logs identify known missing columns
and extract safe column identifiers from PostgreSQL diagnostics, without SQL values.

Next administrator action: identify the database assigned to the live Render service,
then run sql/pm_lease_preflight.sql read-only. Compare reported columns and Alembic
revisions before approving any targeted schema/data compatibility correction.
Do not blindly replay broad sync migrations or stamp migration history.

## R01 association
The local DOCX and PDF match both approved SHA-256 hashes in lease_documents.py.
The DOCX contains Almond. This supports the intended Almond association, but does
not independently establish the production row ID or manager.
After confirming production property 26 is the intended Almond property and document:
- explicitly set PM_LEASE_REVIEW_PROPERTY_ID=26 on the Render service;
- set PM_LEASE_DOCUMENT_ROOT to its existing PRIVATE persistent document root;
- verify root/R01 contains the exact matching PDF/DOCX pair, then restart/redeploy.
These plain environment keys were not previously loaded by Config; this wiring is
included. Neither association nor production configuration was enabled here.
Missing/invalid association is optional and never blocks property selection. It does
not grant access: active-user and manager/property authorization remain mandatory.
Do not infer ownership, tenant identity, or a lessor from the document or property name.

## Implemented interface
The menu and workspace heading use Lease; /billing/leases and existing document/
wizard routes remain unchanged. The redundant extra Property & Owner link is removed.
The vertical workspace has five numbered sections. Selection reads saved authorized
property name/address/current owners. Existing leases and verified R01 delivery remain
usable when compatible. Pages 1-3 are reached via their existing CSRF-protected property
selection POST and remain session-only.
New upload, full details persistence, requirements, Save Draft and activation are
explicitly disabled: there is no truthful persistent implementation on this schema.
No files, tenants, properties, leases or document associations are created as side
effects. Unsupported values are not collected into misleading preview forms.

## Persistence proposal for separate approval (NOT implemented)
Minimum additive candidates, subject to production preflight and model review:
1. Lease draft record: existing property FK, manager/creator, reference, nullable
   existing tenant/lessee linkage, typed dates/rental/deposit/payment due date,
   draft/review status, requirements payload, version and audit timestamps.
2. Private lease document metadata: draft/property linkage, uploader, lease/supporting
   role, private storage key, original filename, MIME, size, SHA-256 and timestamps.
Use the existing private storage convention, never public/static upload URLs. Reuse
existing identities; do not invent tenants to satisfy BilLease. A separately reviewed
activation design must validate tenant linkage and lease invariants and append an
auditable transition; saving/uploading must never activate a lease automatically.
No proposed table or column has been created. Full upload/draft/activation work remains
blocked pending approval of persistence and activation rules.

## Native Print
/billing/portfolio/print now serves the same authorized complete HTML report with a
load handler invoking window.print(). The browser native print dialog opens in the
user-initiated print window. Email retains the existing numbered PDF builder and
standardized sender. No property/owner/municipal data or email recipient behavior changed.

## Verification
Tests use temporary local PostgreSQL records and synthetic private document pairs,
never production data. They cover schema-compatible and missing-column queries,
real 42703 rollback/log diagnostics, property authorization, read-only saved owners,
explicit R01 association, hash integrity, unavailable persistence controls, native
Print HTML, and unchanged property/owner/email behavior.
Production exact column, database identity, Render variables/files, live printing and
deployed native PDF rendering still require administrator verification.
