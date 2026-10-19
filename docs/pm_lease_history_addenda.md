# Lease addenda, signed copies and exact-version email

## Local implementation and existing storage

Use an existing manager-authorized saved lease draft. The Addendum and Lease History submenu actions preserve property/draft identity. No operational lease, tenant, property, master or schema is created/changed. Signed copies and generated addenda use existing `bil_lease_document` records and authenticated Lease R2 UUID keys. Document type, per-type version, actual uploader/time, signature status and original-agreement association are stored under `bil_lease_draft.items._lease_history`. Existing attachments without this metadata are displayed as legacy attachments; their type/version/uploader are not invented. Existing access, upload approval, size/type validation, byte read-back, hash and scope checks remain in force.

Reserve a pending record, JSONB metadata and next version under the existing property/draft lock and optimistic version check, commit before network I/O, then publish only after private upload/read-back verification and renewed access checks. Unique IDs/object keys preserve previous files. Failed/unconfirmed uploads remain pending and inaccessible for reconciliation; no records/objects are deleted automatically. Concurrent draft edits are preserved by a second locked read before publication. Limits are 100 history documents and 100 email attempts per draft; reaching a limit blocks further writes rather than pruning history.

## Original lease and addenda

Record the specific original signed copy and a meaningful agreement reference in Lease History. This is a user declaration, not signature validation. Original upload captures the saved particulars as the historical snapshot. Select that exact original ID for an amendment or signed termination; cross-property/draft IDs fail authorization. Addendum DOCX contains the original reference and SHA-256, original saved parties/representatives and premises, effective date, user-supplied amendment wording, user-supplied continuing-effect provision and signature section. It retains the property master's package/style/page setup but replaces its body; it does not reproduce the master Terms and Conditions. Source header/footer text is cleared to avoid stale particulars. The original master and signed original remain unchanged. Uploaded amendment wording DOCX is independently retained as a private immutable source document before generated output is stored.

Amendment wording may address clauses, rent, deposit, period or other particulars. The application does not automatically apply it to original particulars, silently renumber clauses, activate the lease or confirm authority/signatures. Additional signatory/witness requirements and substantive amendment language require user/legal review. If a file and typed wording are both supplied, reject the ambiguity. No default continuing-effect legal clause is invented.

The December 2023 and July 2024 example addenda were not located in this checkout or its project output files. Comparison against those actual examples and Word visual layout approval remain outstanding. Examples are not simulated or claimed to have been reviewed.

## View Lease, Word and email

The separate Generate Word submenu link is removed; the underlying review and generation routes remain intact. View Lease shows Back, with Email and Print side by side immediately beneath it. The shared header changes only when a viewer supplies a document token. Print calls native `window.print()`; print CSS shows every logical page, resets zoom and hides navigation/actions. Existing logical pages are not claimed to be Microsoft Word page boundaries.

Email opens a recipient/message form and sends nothing on GET or Cancel. A signed, expiring token identifies the selected property/draft, source master/document and output SHA-256. Server-side access and integrity checks run again at submission. Working/standard views reject changed draft versions; later master uploads never change the token's selected source. Immutable stored documents remain selectable independent of subsequent draft edits. The attachment is exactly the selected DOCX/PDF bytes, not a regenerated unrelated master or a conversion.

`app.utils.mailer.send_standard_report_email()` is reused, with optional attachment MIME, document label and message parameters. Existing assessment/PDF callers retain their defaults, sender identity and transport. No new SMTP connection/provider/configuration is introduced. Pending email attempts are recorded before calling the sender; only explicit True records submitted. False/exception records failed/unconfirmed submission. Submission is not delivery confirmation. Duplicate attempts with the same token are rejected; pending outcomes are not automatically resent. If the database is unavailable after a sender result, no reliable atomic transaction exists across SMTP and PostgreSQL; retain the pending attempt for reconciliation and do not retry blindly.

Signed PDFs are retrieved/downloaded as exact private copies and can be emailed through history. They are not converted into an HTML lease, and no PDF conversion service is introduced. Browser-specific PDF opening/printing and actual native print layout require manual testing. The text viewer still does not reproduce full Word pagination/headers/footers.

## Standard lease safeguards and audit limits

The uncommitted AIT Standard Lease development workflow remains separate. Standard viewing/email uses its acknowledged, pinned builder and preserves legal-release restrictions and draft marking. Audit-only `_lease_history` updates do not invalidate its particulars acknowledgement. This application metadata is preserved by application routes; it is not a database-enforced immutable legal audit ledger. No claim of signature validation, approval, activation or issuance is made.

## Deployment requirements

Local only: no commit/push/deployment, production migrations, flag changes or live upload/email. Existing `pm_lease_001`, verified property-master schema, dedicated private Lease R2 configuration/approval and the platform mail configuration are required. No new migration is introduced. A rollback of application code must preserve JSONB history/document records and private objects. Actual R2 roundtrip, real mail submission/delivery, Word layout, browser print output and example-addendum comparison remain production/manual verification tasks. Protected programme source and schema are untouched; source isolation is not end-to-end SACE/HOME/Reading/METSOA testing.

## Changed files for this work package

- `app/program_billing/lease_history.py`
- `app/program_billing/lease_preview.py`
- `app/program_billing/lease_standard.py` (existing uncommitted standard workflow; audit exclusion and viewer token only)
- `app/program_billing/routes.py`
- `app/utils/mailer.py` (optional attachment parameters; existing defaults preserved)
- `templates/program_billing/lease_history.html`
- `templates/program_billing/lease_addendum.html`
- `templates/program_billing/lease_document_email.html`
- `templates/program_billing/lease_workflow_base.html`
- `templates/program_billing/lease_live_view.html`
- `templates/program_billing/lease_submenu.html`
- `tests/test_pm_lease_history.py`
- `tests/test_pm_property_management.py` (isolated module registration)
- `tests/test_pm_property_lease_master.py`
- `tests/test_pm_lease_live_preview.py`
- `tests/test_pm_lease_navigation_pages.py`
- `docs/pm_lease_history_addenda.md`

Earlier local AIT Standard Lease files and unrelated AGENT.md/diagnostic work remain uncommitted and are not implicitly approved for staging.

## Final local verification

101 focused Python test executions passed (including inherited regression cases): 32 history/addendum/mailer/property-master; 30 standard-lease/property-master; 12 saved-data viewer; 8 navigation; 2 merger; 17 Property Management. The history suite was rerun successfully after the final conditional-publication guard. Existing frontend page navigation, zoom limits and boundary checks passed. Syntax and diff-whitespace checks passed. No protected-programme end-to-end claim. No files staged or committed.
