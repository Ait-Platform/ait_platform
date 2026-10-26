# Clause Index and Core Lease Master release

## Observed request flow and evidence

Select Clause requests GET /billing/leases/property/<property_id>/addendum?draft_id=<draft_id>&action=clause_index.
Before this change its first request synchronously assembled the complete Full Lease,
extracted text and saved the index while holding a draft row lock. PDF text dictionaries
also extracted image data that was discarded. These are verified code paths, not a
confirmed explanation of the production proxy 502. No authenticated Render CLI/log
access or production PostgreSQL connection was available during implementation.
Consequently the existence of Almond's production index, exact live exception, memory
peak and worker timeout remain unverified. No production database inspection or update
was performed by this release task.

## Index lifecycle

The first request persists a building job in the selected draft's existing
_lease_history.full_lease_index_job JSON and returns HTTP 202. A bounded executor (one
worker, at most two claimed jobs per process) builds outside the HTTP request and
outside a database row lock. Cross-process claims are serialized in PostgreSQL.
The worker revalidates the recorded active user and property ownership before private
SC/map retrieval. No browser cookies or credentials are replayed. The result persists
in _lease_history.full_lease_clause_index. Source version is the
actual compiled PDF SHA-256; inputs/renderer fingerprints invalidate stale indexes.
Assembly page spans identify sections without assuming that a future Core has nine
pages. No original contractual wording is used from an SC database record when an
uploaded SC PDF is authoritative.

The exact assembled PDF is cached as immutable authenticated R2 objects under the
selected property's/draft's clause-index prefix, with per-part and complete SHA-256
checks. They are derived copies; original PDFs and signed documents are untouched.
Subsequent visits retrieve the index and cached PDF without re-rendering or extracting
text. Checkbox, PDF and preview requests reject a stale/not-ready index rather than
performing a cold build. The obsolete live-search endpoint returns JSON HTTP 410. Word/number/title filtering and Enter handling stay entirely in the browser.
Failures log the backend traceback, retain saved selections, and expose Retry Index.
A lost worker becomes explicitly retryable after five minutes. No external queue,
new service, environment setting, database table or migration is introduced.

## Core source and publication

The existing approved private AIT_Residential_Core_Final_Review.pdf is version 1:
9 pages, 100272 bytes, SHA-256
3442a3a250ddd7fde1fabd6040409b338d0bbd302b6014a84073c3c0a196441c.
Its original publication date is not recorded; the UI says so instead of inventing it.
The original object, legal wording and hash pin are unchanged.

The existing PM administration dashboard links Core Lease Master only after the same
active User + active ApprovedAdmin grant policy used by Platform administration.
Every master operation independently enforces that policy, as well as existing CSRF.
An ordinary PM role or landlord's admin-like role is insufficient.

The administrator downloads a working DOCX, edits in Word, saves that revised DOCX,
then publishes its PDF rendition. Version 1's Word download uses the existing
PDF-to-Word facility and carries its conversion limitations. Later Word downloads use
the paired working DOCX. PDF publication is the sole generation source; working DOCX
files are preparation records, not competing generation templates. The server does
not claim to prove that an administrator's supplied PDF equals their DOCX rendering.

Publication manifests and document objects are immutable in the existing private R2
bucket under private/leases/core-master/. Sequential conditional-put manifest keys
prevent competing publications from overwriting each other. A manifest records the
version, previous version, administrator, date, PDF hash/size/page count and paired
DOCX. No mutable current pointer or additional database schema is needed. The highest
valid published manifest is current. Existing R2 object read, write and list permission
is required. Storage/permission failures do not silently choose a different master.

Both fresh-draft creation paths save an explicit _core_master pin in existing draft
JSON. Unpinned pre-release drafts always mean legacy version 1. Forks preserve their
existing pin. New publications do not update existing drafts. An explicit administrator
action can update an eligible draft, with optimistic locking and a previous-version
record in _lease_history.core_master_updates. Signed/issued particulars are excluded.
Previously generated PDFs, signed originals, Addendums and SC objects remain unchanged.

## Operational limitations

Render deployment and live verification must be checked separately; a successful Git
push is not proof of deployment. Initial work still needs enough memory in the web
process, though it no longer occupies the selection request and skips image extraction.
Worker restarts are recoverable through persistent state and controlled retry. Image-only
pages remain unsearchable without OCR; the index reports them and invents no clauses.
