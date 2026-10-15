# Menu 2 Lease delivery and schema review

## Implemented

The main Lease page contains five numbered sections; after property selection the remaining sections are navigable with the property and draft IDs preserved. Dedicated input pages do not repeat the programme menu. Existing Lease Pages 1-3 and R01 delivery routes are unchanged.

A-E groups use every original Schedule 1 catalog key exactly once. Important source discrepancy: the verified R01 DOCX contains **41 unnumbered Schedule 1 rows**, not the 25 numbered items in the request. Its existing s1-01...s1-41 keys and all original placeholders are unchanged. Grouping is by meaning: Parties rows 1-9,39,41; Premises 10-16; Period 17-20; Amounts 21-38; Conditions 40 and SC-01...SC-10. These row ordinals describe catalog positions, not new contractual numbering. A verified numbered source is needed before assigning alternative legal item numbers.

Parties presents current authorized registered owners for explicit contracting-lessor selection and confirmation. Saving captures identity, identification, contact and address details into the existing draft JSONB. Current register edits never automatically rewrite that saved contracting snapshot. Lessee details use one modal; no tenant, owner, property or lease records are created as a side effect.

Grouped saves preserve unrelated draft items and use property/draft locks, the existing readiness gate and optimistic version checks. They save partial particulars; Completion Check separately identifies essential/conditional gaps and remaining placeholders. Period checks use explicit months/years and inclusive expiry (anniversary minus one day).

Supporting classifications and applicability explanations are saved in the existing items JSONB; private uploads reuse the existing scoped document metadata/R2 adapter. No public URLs are used. References to explicit Core clauses, SC identifiers, schedules and annexures are checked against a manifest derived from the hash-verified source. Additional clauses explicitly continue after Core 28; invalid sequences are rejected, never silently renumbered. Exact textual duplicates of master clauses are rejected; substantive duplication, ambiguous legal wording and legal effect still require human review. Checks are conservative lexical checks, not a legal opinion or a complete natural-language reference resolver.

## Stage G: isolated merge engine

`lease_merge.py` is a pure, separately tested DOCX byte transformation, not a generation endpoint. It preserves the input master, checks its expected approved hash, replaces bracket placeholders across runs and headers, retains XML namespaces/formatting, and returns distinct output bytes, hash and a copy of values for future audit persistence. It rejects missing/unknown/unresolved mappings and unsupported Word MERGEFIELD instructions. It does not select or approve a master, persist output or activate a lease. No Generate button is exposed prematurely. R01's generic repeated placeholders and combined values (amount/words, extent/unit count, execution details) require an explicit approved per-location mapping before production generation; they must not be guessed from a single free-text value.

## Minimum additional persistence proposal (review only)

The existing proposed `pm_lease_001` draft/document tables remain unchanged and outside automatic Alembic discovery. They support particulars and lease-specific supporting uploads, but cannot safely represent a reusable platform master or immutable generation history without misusing a property-scoped draft record.

For a later reviewed migration, propose:

- `bil_lease_master_version`: UUID identity; logical master family/version with unique family/version; unique private storage key; filename/MIME/size/SHA256; creator and creation time; approval provenance; immutable placeholder/location mapping, clause/reference manifest and source hash. Published versions must not be overwritten or deleted. Replacement creates a new version, uploaded/approved only by authorized administrators.
- `bil_lease_generation`: UUID identity; authorized draft/property/manager composite FK; master-version FK; draft revision; immutable merged-value and contracting-party snapshots; SHA256/file metadata/private object reference; creator/time and pending/available/failed state. Unique draft/revision/master/input hash prevents duplicate generation. Restrictive foreign keys and immutable audit rules preserve history.

Approval/publication provenance and revocation must be audited; an approved master version must be reusable across authorized drafts without linking it falsely to an arbitrary property. Detailed DDL, ancestry, privileges, constraints and migration tests require review before implementation/application. No new schema or production records were changed in this delivery.

## Remaining activation prerequisites

1. Inspect actual Render schema, migration history, property identity and existing document root using authoritative access; obtain/verify a backup and rehearse the proposed draft migration on a restored copy.
2. Approve/apply the reviewed draft/document schema and enable `PM_LEASE_DRAFT_WRITES_APPROVED` explicitly. It remains false by default, with schema/revision checks still required.
3. Verify the actual general `R2_BUCKET_NAME` and separate `SACE_HOME_R2_BUCKET`. The repository documents `ait-platform-assets` for public Reading delivery, but this is not verification of Render's current configuration. Verify private lease objects are inaccessible through *every* public access path, including r2.dev, without disrupting Reading. A private-looking key prefix alone provides no privacy. Approve `PM_LEASE_PRIVATE_STORAGE_APPROVED` only after this succeeds, then verify a real authenticated upload/download/retrieval on Render.
4. Review and approve the master/generation schema and a legally approved master with an unambiguous per-location merge map. Then implement versioned master upload, approval and persistent generated-document delivery as a separate stage.

No production schema access, bucket/privacy verification, R01/Almond association, migration execution or Render upload roundtrip is claimed. Current process bucket/root configuration is unavailable. Master uploads stay disabled even if the draft and storage flags are enabled. Completion Check is functional independently; it never declares the lease legally valid or ready for signature.
