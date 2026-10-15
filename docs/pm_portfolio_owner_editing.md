# Property Portfolio and owner editing verification

The Portfolio, clean Print document and emailed PDF use the same escaped report
partial. Reports query manager-authorized, non-archived properties only, preserving
the existing Portfolio visibility restriction. Scalar bil_property records are read
without loading municipal accounts, meters, tenants or leases. Ownership versions
include current identity details, assignment snapshots, shares, effective dates and
recording timestamps. Superseded versions are explicitly labelled historical.

Email reuses app.utils.mailer.send_standard_report_email and
app.utils.pdf_render.html_to_pdf_bytes. It uses the configured platform sender and
opens an editable recipient modal. Only explicit Send Email submits the report
to the validated entered address. Opening or cancelling does not invoke the sender.
The report content remains scoped to the authenticated manager, independently of
the entered destination. Success identifies the normalized destination; failures
retain it and reopen the modal. Suppressed sending, PDF errors, exceptions and a
non-True sender result never produce a success flash. Confirmation means submitted
to the configured sender, not proof of mailbox delivery.

Save Changes trace:
- Existing identity edits already persisted in local endpoint tests; the reported
  production failure has not been independently reproduced.
- Confirmed form defects: Edit Property also wrote address data; the owner modal
  disabled percentage input and the owner endpoint discarded percentage changes.
- Property updates now write only name and archive status.
- The owner modal submits the selected owner's identity fields and explicit
  property/assignment IDs. Shared identity changes remain creator-authorized.
  Active and percentage changes apply to that assignment only, with property
  manager authorization, row locking and append-only assignment supersession.
- Identity and assignment updates commit together; invalid assignment inputs roll
  back identity edits. Stale assignment versions return a useful conflict response.
- Browser validation and JSON errors keep entered details in the open modal.
  Success is flashed only after commit, followed by refresh from persisted data.
- Individual, company and trust identity edits and property-scoped shares are
  covered by PostgreSQL regressions, along with identity reuse and three-owner edits.

Inactive assignments remain editable via Manage owner assignments, separately from
the deliberately minimal Edit Property form. No schema or activation flag changes.

Local checks: focused property/owner/configuration/navigation tests; executable
owner modal tests. Mail/PDF calls are mocked in integration tests: no email is sent.
The broader 54-test Lease Viewer suite cannot initialize because the private
output/pm_stage5/Lease_Review_Draft_R01.docx fixture is unavailable. Lease/METSOA/
Occupier business logic was not changed.

Production still requires verification of the deployed revision, the actual owner
Save Changes request/response, approval/schema readiness, PDF renderer availability,
the clean Print output and real sender submission/mailbox receipt. If the live edit
failure persists, capture its request status, JSON error and matching server error
class without including owner identity/contact values.

## Recipient correction investigation

In commit 5f0e2c40 the Email button immediately posted without a recipient form.
The endpoint always selected current_user.email and ignored request recipient data.
It called send_standard_report_email only after PDF generation succeeded and mail
was not suppressed. No production logs were available to prove whether an
administrator's particular live click reached that sender. Local tests confirm
the standard sender is invoked with the entered destination after explicit submit.
All local mail and PDF calls are mocked: no real email is sent during tests.
