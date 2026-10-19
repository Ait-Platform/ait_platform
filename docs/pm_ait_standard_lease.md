# AIT Standard Lease development stages

## Inspected architecture

`app/models/billing.py:BilLease` is the operational lease, including mandatory tenant ID and historical billing fields. It is untouched. Draft and attachment workflows use explicit SQL over `bil_lease_draft` and `bil_lease_document` with the `pm_lease_001` schema and PostgreSQL JSONB/version checks; no corresponding ORM classes or exact `bil_lease_doc` table/model were found in this checkout. `bil_property_lease_master` uses explicit SQL, version allocation/property locks, verified state, scoped keys and private R2 read-back integrity. No additional ORM or operational lease records are required for this development stage.

Existing routes/templates include `lease_workflow.py`, `lease_preview.py`, `lease_generation.py`, `lease_property_master.py`, `lease_storage.py`, `lease_r2.py`, the five particulars templates, review template, submenu and viewer. Existing property-specific viewing, legacy Word generation and uploaded masters remain unchanged.

## Stage 1 implemented locally

An opt-in AIT Standard Lease working-copy route is linked beside the existing Generate Word action. The original eight submenu choices remain intact. It uses an existing persistent draft and the selected property's latest available/verified `Lease_Review_Draft_R01.docx` on its first save. After saving, the exact property/master ID, version and SHA-256 are pinned in `items._ait_standard.source`. Later uploads do not replace that reference. Private R2 approval and integrity checks remain mandatory. There is no fallback to Stage 5 or another property. A canonical shared standard for other properties must be an explicitly reviewed future release, not an implicit use of another manager's master.

Source filename is a development-reference selection check, not legal approval. Actual production R01 contents and section boundaries have not been inspected in this session. The development document adds Cover, Lease Certificate and Terms and Conditions headings but retains the complete reference body under Terms and Conditions, including its original schedules/signatures. This deliberately avoids guessing away substantive text. Removing redundant source front matter and publishing standalone operative terms require actual R01 comparison and explicit review. Do not present the assembled working copy as the final reviewed standard.

## Merge dictionary and structured particulars

`app/program_billing/lease_standard_fields.json` is the versioned machine-readable dictionary for all 37 existing exact bracket fields, with semantic names, source paths and execution-only classification. Existing mappings provide identities, notices/contact particulars, property, premises area, plans, commencement/occupation/expiry, ZAR rents and adjustments, deposits, parking, utilities, banking and signature particulars. Missing values remain unresolved. No names, dates, commercial terms or authority are invented. This dictionary is inherited from the existing saved-particulars mapping and must be compared with the actual R01 upload: unknown source tokens remain visible and block signature readiness; native Word MERGEFIELD instructions fail clearly until separately mapped.

The Certificate separately lists every saved contracting landlord and tenant, and their representatives and authority evidence. It also includes all saved additional special-condition text with its existing number; no source clause is silently renumbered. Authority confirmation is bound to the representative fingerprint and requires a supplied name, ID, capacity and evidence reference. It is user-recorded verification, not AIT's legal approval. Natural persons are not automatically given entity representative authority. The dictionary intentionally does not transform arbitrary saved form fields into new contractual terms; extra premise scope, permitted-use or annexure details without reviewed merge slots require completion/legal review.

Both standard View and Word use `build_document()` and the same exact-token merger. Standard viewing performs no database writes. Word generation saves an output hash, source/dictionary versions and value snapshot through the existing optimistic draft save before successful download. Existing property View and Generate Word behavior is untouched. No PDF service, dependencies, public document URLs or new buckets are introduced.

## Notice, applicability and readiness

The supplied Important Notice is used verbatim, with development version and SHA-256. Acknowledgement records user/time, disclaimer version/digest, exact source digest and particulars digest, with bounded append-only-in-application history. Particular changes invalidate the acknowledgement and require re-acknowledgement. This is application audit history in draft JSONB, not a new database-enforced immutable audit ledger.

Every clause is mandatory. No exclusion input is presented. Forged exclusions, mismatched source/inventory digests and unsupported native fields fail closed. A descriptive numbered inventory and cross-reference warnings are NOT an approved excludability register. `lease_standard_release.json` contains no legal approvals and no permitted exclusions. Any future optional-clause implementation requires an approved register keyed to exact source digest and review of dependent references; it must not be activated by changing a checkbox or guessing from clause numbers.

Readiness requires complete mandatory particulars, identities/authority evidence, source-numbering/reference checks, an unchanged acknowledgement and independently approved terms, disclaimer and all-mandatory policy with exact digest/register matches. Missing legal release approvals always block Ready for Signature. Its eventual status is a version-bound draft metadata designation, not an operational lease activation, signature or issuance. Generated working copies remain marked DRAFT/NOT FOR SIGNATURE OR ISSUANCE in this release even if readiness validation is later exercised in tests.

## Storage and migration impact

No migrations or schema changes. New metadata lives only under existing `bil_lease_draft.items._ait_standard`; other saved parts and histories are preserved. `bil_lease_draft.status` remains `draft`, satisfying its current database constraint. No changes to `bil_lease`, properties, owners, tenants, private-master rows or other programmes. A verified standard master, release registry and full legal audit history may warrant a separately approved schema later; none is created here.

## Deployment prerequisites and remaining stages

Do not deploy until reviewed. Validate actual latest Almond R01 and its package, tokens, operative terms/signature/schedule boundaries and internal references. Review the proposed extraction into a genuinely separate standard terms asset against the unmodified source before removing any source front matter. Obtain legal approvals for terms version, notice and all-mandatory applicability policy. No exclusions are supported in this development release.

Confirm production draft/master schema, authenticated R2 retrieval and dedicated Lease bucket approval independently; do not change flags or run migrations as part of this work. No production access is available in this session. This implementation does not establish production readiness.

Run focused standard, property-master, draft/merge/navigation and Property Management regressions. The synthetic DOCX is written only to local `tmp/ait-standard-synthetic-DRAFT.docx` (not staged or committed) for review. Word/visual rendering is not verified: LibreOffice is absent locally. No converter/dependency/hosting change is introduced. Existing protected-programme source and schema must remain unchanged; source isolation checks are not end-to-end SACE/HOME/UIP testing.

## Local verification results

Final standard-workflow suite: 30 passing tests, including the existing 20 property-master/security checks and 10 standard/policy checks. Separate regressions: 12 saved-data viewer, 8 navigation, 2 merge and 17 Property Management tests passed. Existing viewer frontend navigation/zoom check passed. No protected-programme end-to-end claim. Document visual QA is blocked: the installed render helper requires missing `pdf2image`, and LibreOffice is absent. No dependency installation was attempted. The synthetic DOCX package/content checks passed; Microsoft Word rendering remains unverified.
