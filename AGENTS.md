# AIT repository instructions

Read and follow [AGENT.md](AGENT.md) for the established repository, application,
database and UI rules. This standard does not authorise production migrations,
financial changes or rollout to another program.

## ProTrade local-folder rollout

ProTrade local folders are explicitly authorised (22 November 2026). Reuse the
existing AITDirectories store and stable owner markers, PM historical viewer and
isolated print adapter. ProTrade adapters must not modify PM files, paths or
behaviour. Each consultant's main folder uses the existing user/protrade key;
create consultant/business/type directories only on successful document saving.
Use Quotes, Invoices and Statements; retain read-only access to earlier marked
layouts. Historical viewing/printing/emailing must never regenerate documents
or deduct tokens. ProTrade history operations recheck authenticated consultant
and business ownership, including stale tabs after account changes. Apply the
forward-only `protrade_local_files_020` migration before enabling historical
email: it allows debtor-free history receipts while retaining ordinary Output
debtor requirements. Do not execute it without production approval. No Franchise
functionality is authorised by this rollout.

**Shared folder core (22 November 2026):** `window.AITDirectories` in
`app/static/js/pm_local_directories.js` owns HTML name selection, dropped-directory
handles, permission approval, protected registration, programme initialisation and
PDF saving. PM and ProTrade UI adapters call `selectFolderName`,
`selectedFolderName`, `droppedDirectory`, `register` and `initialise`; do not add
another store or picker. HTML selection grants no write access and cannot identify
an empty folder; retain the drop-and-click approval path. PM retains its existing
initial report folders and first-save categories. ProTrade creates only the
stable-owner consultant folder during initial authorisation; business and document
folders remain first-save only. Names come from authenticated server metadata,
never hard-coded people. Existing assignments cannot be replaced by registration.
Business/report metadata remains an allowlist, not a document registration: list
only destinations found under the authorised, correctly marked main folder.
No Add Folder or Franchise functions are included in this core consolidation.

## Reusable AIT folder-management standard

Use the Property Management workflow for future authorised folder development:

**Select Main Folder → Assign → Create Subfolders → Save Reports → Folder Register → View Files → View PDF → Print / Email**

This standard guides future ProTrade, BudgetCash, Lease Management, CRM Health,
Reading and other existing or future AIT programs; it does not authorise changing
those programs now. Automated verification and live Windows verification are
separate: never describe an untested browser/OS workflow as proven live.

### Selection, authorisation and protected assignments

- Assign one main folder to stable authenticated user and authorised property,
  business or client IDs. Server configuration includes only contexts belonging
  to that user. Never accept local filesystem paths or another user's assignment
  from an HTTP request. Display names are not ownership identifiers.
- Reuse the PM selection/authorisation flow in `pm_folder_name.js`. HTML
  `webkitdirectory` selects and displays a name only; it does not grant writable
  access. Never upload its selected files, persist a FileList as an assignment,
  or reintroduce the retired `showDirectoryPicker()` picker. The staged
  `pm_property_folders.js` remains inactive.
- For initial writable setup, an explicit folder drop onto Select Folder obtains
  `getAsFileSystemHandle()` during the drop event. Clicking that same button
  requests write approval through a genuine user gesture. Verify granted write
  permission, reject same/nested/parent destinations assigned to other contexts,
  and persist/read back the real directory handle. A name alone proves nothing.
- Reuse `app/static/js/pm_local_directories.js` (`window.AITDirectories`) and its
  existing IndexedDB user/property assignments; do not create another handle
  store or delete/migrate legacy assignments. Automatically restore the saved
  folder name, hide Select Folder for assigned contexts, and prevent accidental
  replacement. Recheck for a concurrent assignment under the origin-wide lock
  before initial setup. Storage-read failure must not enable replacement.
- Never request permissions on page load. Query permissions without prompting;
  Allow Access requests read access for the already assigned handle on a click,
  without another picker or assignment change. Preserve existing write renewal
  through Save Report. Handle denial, expired permissions, unavailable APIs,
  IndexedDB failure and missing Web Locks clearly. Preserve Download PDF when
  direct saving is unavailable; never silently use another context's destination.

### Subfolders and new reports

- Each program defines its own required report categories and safe directory
  names. Never copy PM's categories into unrelated programs. Preserve authorised
  initial-setup/restoration creation for PM's legacy destinations only. New
  registered paths are created on first Save Report with verified write access.
  Reuse existing folders; browsing, reading and read-permission renewal must
  not create directories.
- Preserve PM's existing per-user/property main folder and `source / report`
  hierarchy, report filenames and registered catalogue destinations. Do not move,
  wrap, rename, delete or migrate existing directories. New programs may use
  `base / user / client / document type` with stable identity markers to avoid
  merging same-named clients and to retain paths after display-name changes.
- Keep generation and saving of new reports in the program's Output section.
  Validate the generated PDF before creating its destination. Reuse origin-wide
  locks and collision-safe filenames; never overwrite historical documents.
  Declare Saved only after write and close succeed. A started browser download
  is not proof that it completed. Preserve PDF generation and download fallback.

### Automatic Report-Folder Creation

- Every future report-producing function must register its destination beneath
  the authorised main folder. PM's `utilities_output_reports.report_destinations()`
  is the shared source for Output saving and Folder Register. Register the report
  in `CATALOG` / `SOURCES` and declare its path in `REPORT_PATHS`; do not maintain
  a separate hardcoded folder register. PM retains its seven legacy paths and
  adds `Link Meters` as the eighth destination for Linked Meters reports.
- On Save Report, validate the PDF and actual write permission first, then create
  any missing registered path components and write/close the document using the
  shared saver. Reuse existing directories and collision protection. Future
  explicitly registered paths are created on first save, not on selection,
  initial folder setup, navigation, browsing or permission renewal. A failed
  write must not be reported as saved; directories may remain after a failed
  write, but existing reports must remain unchanged.
- Folder Register discovers registered destinations automatically, reads actual
  counts and refreshes after a completed save. Missing/unreadable destinations
  show a dash, not a false zero. Never create folders for unrelated functions
  that do not produce saved reports.
- This principle guides future PM, ProTrade, BudgetCash, Lease Management,
  CRM Health and other report-producing AIT programs. Each retains its appropriate
  structure and stable ownership IDs. Do not modify other programs without a
  separate authorised task; ProTrade changes remain subject to the explicit rollout scope above.

### Folder Register and historical documents

- Follow the AIT permanent-hub pattern: established colours, borders, spacing and
  typography, one collapsible working area, and Close returning to the hub.
- Folder Register is for browsing/retrieving history, not report generation.
  Use numbered clickable folder names with actual file counts. Count files in
  the assigned destination only. Show zero only for a successfully read empty
  folder; show `—` when read access or the destination is unavailable.
- Clicking a folder lists actual filenames and available dates. Clicking a
  filename opens the original saved PDF in the same working area. Provide
  Back to Folder Register in the list, and **Back to Files, Print, Email, Close**
  in the viewer. Revoke local object URLs when leaving the document view.
- Read historical bytes directly from their original authorised directory.
  Never regenerate, recalculate, rewrite financial snapshots, deduct tokens,
  rename, modify, delete, move or duplicate a historical PDF during retrieval,
  printing or emailing. Preserve old-path retrieval where layouts have evolved.
- Print uses the original saved PDF bytes through the existing PM isolated
  PDF.js print adapter and browser print dialog, never the hub webpage. Do not
  reintroduce the failed native PDF iframe printing path or claim printing completed. Email uses the existing AIT mail service,
  the exact original bytes and original filename, with an entered recipient.
  Only an explicit Send Email uploads that selected attachment for delivery;
  do not upload paths, directory handles or unrelated local files. Require
  authentication, context ownership, CSRF, validated recipient/PDF/size, durable
  duplicate-send protection and truthful delivery/uncertain-outcome messages.
- Reuse established historical-viewing integrations where practical:
  `templates/partials/local_document_history.html` and
  `app/static/js/document_folder_history.js`. PM's single-area adapter is
  `pm_folder_register.js`; do not alter other programs to impose its UI.

### Verification

Test protected assignment restoration, initial setup, user/context isolation,
same-named clients, overlap rejection, permission denial/renewal, unsupported
browser fallback, authorised subfolder creation, actual counts and empty versus
unreadable folders. Verify original bytes through view/print/email, filename
preservation, duplicate-send prevention, failed writes, filename collisions and
unchanged Output saving. Shared API changes require relevant PM and ProTrade
browser regressions. Explicitly report remaining live Windows, native printing
or real email verification; automated tests alone do not establish those results.
