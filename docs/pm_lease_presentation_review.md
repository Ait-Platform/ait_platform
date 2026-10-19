# Lease presentation and navigation - local review

## Scope

Eight shared submenu items: Parties, Premises, Period, Amounts, Special Conditions, View Lease, Addendum, Lease History. Generate PDF and the separate Lease Certificate menu link are removed; their existing routes remain. Upload Master Document moves to a separate Document administration disclosure on the selected property workspace, with existing property authorization and private-storage checks unchanged. Working-copy settings/notice remain accessible there; no legal acknowledgement gate was bypassed.

Premises, Period, Amounts, Banking/Utilities and Special Conditions use simpler labelled presentation. Input names, existing required fields, dates, monetary validation, conditional applicability, optimistic saves and existing JSONB keys are unchanged. Rendered-form submission/reopen checks preserve all 42 Certificate row values and the existing parties, ownership snapshots and standard metadata.

## Private Core Terms PDF

GET /billing/leases/property/<property_id>/standard/terms.pdf?draft_id=<uuid>&revision=<version> uses the same authorized saved draft, current development acknowledgement and exact pinned verified source. Its link is in Document administration. No database writes or generation-history entries occur. Stale revisions, unauthorized properties, changed source hashes, failed private retrieval and unsupported document structures fail safely without substituting another master.

ReportLab is already in requirements; its bundled Vera fonts are embedded. A4 layout has repeating AGREEMENT OF LEASE, full saved landlord/tenant names, development-only notice, source version/hash, per-party initials spaces, and Page X of Y. All source paragraphs/tables and embedded reference images are retained; literal clause numbering is formatted without renumbering. Automatic Word numbering and unsupported inline image/object structures fail safely instead of losing content. Original documents are not edited.

The existing Core Terms is still the complete development reference, including schedules and signatures, rather than a separately approved operative-core asset. This PDF explicitly retains that material; it does not guess section deletions or correct pre-existing legal numbering. Source-specific unknown placeholders remain visible. A separately maintained/approved core asset and any renaming of unmatched source fields require separate review. No Ready for Signature approval is conferred.

## Verification

106 focused test executions passed: presentation/PDF/form roundtrip/journey 5; simplified standard/history/security 62; saved viewer 12; navigation 8; merge 2; Property Management 17. Updated the old ten-item menu expectation to eight. Viewer frontend reflow/page/zoom checks passed. PDF verification checks every extracted source paragraph, repeated party names/initials slots and every Page X of Y footer. Synthetic local review PDF: 29 pages, 2 reference images; all-page contact sheet and full-size first/middle/final page review completed. Header clearance corrected after visual review.

Four additional legacy offline suites have 38 pre-existing setup errors: Period 7, Premises 9, Amounts 11, Banking/Utilities 11. Their isolated fixture cannot import the already-existing tenant_register dependency before any route executes. Both that shared fixture and lease_workflow.py are unchanged from HEAD. The current PostgreSQL-backed rendered-form roundtrip test directly covers saving/reopening the affected forms; no unrelated application edits were made to accommodate obsolete fixtures.

Unchanged: View Lease template/merge path, Email and Print controls, mail sender, Word generation, Parties template, master DOCX, release approval registry, database schema, protected programmes. Master SHA-256 remains 98b391c497cb2c70f931a6117a3b0074776256faaa8be91f3b6b9f4ad5d12937. No production access or records changed. No commit, push or deployment. No new dependency, service or hosting requirement.

## Files

- app/program_billing/lease_standard.py
- app/program_billing/lease_terms_pdf.py
- templates/program_billing/lease_submenu.html
- templates/program_billing/lease_management.html
- templates/program_billing/lease_particular_group.html
- templates/program_billing/lease_certificate_field.html
- templates/program_billing/lease_premises_structured.html
- templates/program_billing/lease_amounts_structured.html
- templates/program_billing/lease_banking_utilities.html
- tests/test_pm_lease_presentation.py
- tests/test_pm_lease_navigation_pages.py
- tests/test_pm_property_lease_master.py
- tests/test_pm_property_management.py
- docs/pm_lease_presentation_review.md

PDF/screenshots under output/pdf are local review artifacts, not deployment assets. Earlier unrelated AGENT.md/diagnostic changes are excluded from this scope.
