# Final simplified Lease design - local review

## Changes

The existing addendum route/form now has one amendment content area, or a DOCX wording upload. There are no separate effective-date or continuing-effect inputs; users include anything required in their own wording. AIT does not interpret, rewrite, approve or apply that text to original particulars. Existing stored addenda and their older metadata remain untouched.

A generated addendum has three parts: a numbered heading identifying the exact original signed-copy ID/hash/reference, property and contracting parties; the unchanged user content; and individual landlord/tenant signature sections, including recorded representative name/ID/capacity and blank place/date/signature lines. Numbers are allocated under the existing lease lock, bound to the exact original signed agreement, and recorded separately from document version numbers. Uploaded wording remains an immutable source document; generated output gets another unique object.

The AIT Standard Lease working copy uses a simple display-abbreviated cover, the full legal Lease Certificate and a version-pinned core-terms development reference. Every Certificate label is explicit in lease_standard_fields.json, identifies its saved source and links to the corresponding existing particulars form. Full legal identities are not abbreviated in the Certificate or saved records. An existing recorded short name is used on the cover when available; otherwise deterministic display initials are used, with surname retained for known natural persons. This is display formatting only, not an asserted legal alias.

Read compatibility for selected tenants is shared by Certificate/addendum generation. Missing optional historical tenant fields become blank only in a deep copy; existing JSONB and original signed copies remain unchanged. An existing single-tenant mirror is not allowed to override the selected tenant array.

Drafts already using the standard working copy now reuse the main View Lease entry and the same standard builder for viewing/email/Word. A failed or unacknowledged standard selection cannot fall back to another master. Existing property-specific master viewing remains available for drafts not switched to the standard. Original generation routes and immutable document-history records remain; the separate Generate Word menu link remains absent.

Email/Print stay adjacent below Back, using the exact selected document version and the existing standardized sender. No new mail service, external AI, clause classification, amendment analysis, approval mechanism, schema, deployment flag or production change is introduced. The existing legal-release restrictions remain unchanged. The visible diagnostic clause inventory has been removed; the prior all-mandatory integrity policy remains enforced internally.

## Core terms limitation

No separately approved AIT Core Terms asset referring to the Certificate has been supplied/verified. The reference is explicitly marked as development-only; the original Almond wording is not silently rewritten, extracted or declared approved. Creating the final maintained operative-core asset and its approved Certificate references remains blocked on that document/review. This local implementation must not be represented as a complete approved standard lease or Ready for Signature. Existing release flags remain false.

## Scope and review

This stage changes lease_history.py, lease_standard.py, lease_preview.py, lease_standard_fields.json, the existing addendum/standard/submenu templates, and focused tests. No new workflow, database table or migration. All originals, private-storage controls and unrelated programme sources remain intact. Earlier uncommitted Lease work is still local and not implicitly approved for staging.

Historic addendum examples and native Word rendering are still unverified as documented in pm_lease_final_verification.md. Browser print checks use synthetic local fixture documents; they are not production verification. No commit, push or deployment.

Certificate mapping includes all 37 existing exact merge fields plus five already-saved inputs: cadastral description, lease scope, included portions, excluded areas and special conditions. Each of the 42 labels links to its existing form; no new data register or input workflow. Signature sections are now per contracting landlord/tenant, with recorded representatives only and no inferred authority.

Final results: 95 simplified/final-verification test executions passed after the last changes; the 39 saved-data viewer/navigation/merge/Property Management regressions passed. Existing frontend page/zoom checks passed. Browser verification of both regenerated three-part synthetic addenda passed: Back/actions alignment, native print invocation, all source body paragraphs present and no printed navigation. These are local fixture checks; actual historic examples, native Word pagination and the final approved Core Terms asset remain outstanding.
