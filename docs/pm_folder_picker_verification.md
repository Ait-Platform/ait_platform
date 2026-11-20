# PM folder picker verification — 19 November 2026

## Clean rebuild status — 20 November 2026

The operator reproduced native AbortError outside AIT in both read and read/write
tests. A JavaScript rewrite cannot establish a working native selection method.
The original picker and diagnostics are no longer loaded by the Folders page.
Select Folder is paused explicitly; no stored assignment is deleted or silently
displayed as a newly selected folder. Existing report-saving behavior remains
unchanged during this gate.

The independent `app/static/js/pm_property_folders.js` module uses a separate
`ait-pm-property-folders-v1` database, `propertyFolders` store and compound
`[userId, propertyId]` keys. It never reads or migrates the legacy database and
does not use `AITDirectories` or ProTrade code. It is not connected to production.
`tests/manual/pm_property_folders.html` is a local-only native verification harness
with the four requested controls. It does not share the production origin/login.
Browser tests use a stubbed native picker with real Chromium handles and IndexedDB;
their success must not be reported as Windows picker success.

Before activation, an actual supported browser must return a native directory
handle, successfully retain it across reopening, and permit a test report write.
Only then wire the Folders page and report-save adapter to the new module together.
Do not activate just one side or fall back automatically to an old assignment.
No database migration, Render setting change, storage deletion or report move is
required or authorised by this rebuild. Earlier findings below are historical.

## Observed failure and limits

The live HTTPS page `/billing/directory-register?property_id=27` showed Dale,
`No folder selected`, and a native `showDirectoryPicker` AbortError. This is a
picker rejection before a directory handle reaches PM; Save and IndexedDB have
not run. It is not evidence of a lost persisted handle. The native picker opened
in the operator's reported workflow, so missing API support is not established.

The UI automation could read Chrome's existing page but could not operate its
controls (`coordinate input geometry is unavailable`); the browser connector was
also unavailable. No successful Windows native-picker retest is claimed.
Chromium browser regressions below stub the OS picker and use genuine browser
directory handles and IndexedDB through origin-private storage.

The browser's AbortError does not identify whether the dialog was dismissed,
access refused, or the selected directory restricted. The exact underlying native
abort reason remains unconfirmed. See the browser API's documented exceptions:
https://developer.mozilla.org/en-US/docs/Web/API/Window/showDirectoryPicker

## Focused correction

PM's earlier implementation (`9cfd2493`) requested
`{mode:'readwrite',id:'ait-pm-reports'}`. The subsequent staged-Save implementation
changed it to `read`, while Save still requested read/write access. Restore PM's
earlier request for its document-saving workflow. This is a compatibility repair,
not proof that permission mode caused the production AbortError.

Keep the picker synchronous with the direct click, before any asynchronous storage
operation. Check secure context, API support, storage availability and user
activation, reject unusable returned handles, and explain each known exception.
For ambiguous AbortError, explain that storage never ran and preserve the browser's
own error detail rather than asserting cancellation or inventing a cause.

The handle is displayed immediately on success and remains unsaved until Save.
The existing user/property IndexedDB key, permission handling, Allow Access,
View Folder, lazy report directories and historical files are unchanged. There are
no server-side filesystem operations or database migrations. ProTrade's ordinary
download workflow is retained. PM asset version URLs are updated to avoid stale JS.

## Verification

Focused real-Chromium tests verify the original picker options and active gesture,
immediate folder name, explicit Save, reload persistence, permission denial and
retry, cancellation with a previously saved assignment, storage failures, missing
gesture, missing capabilities, malformed picker results, View Folder, Allow Access,
user/property separation, historical viewing/printing and lazy directory behavior.
PM layout and ProTrade release regressions also pass.

Results: 31 tests and 13 subtests passed across `test_pm_directory_register.py`,
`test_folder_selection_stages.py`, `test_pm_metsoa_folder_save.py`,
`test_protrade_output.py`, `test_pm_directory_layout.py` and
`test_protrade_boq_folders.py`. The METSOA integration test initially skipped
without its explicit local connection; rerunning with the current `.env` passed
under the fixture's localhost/`ait_local_db` guards. It exercised genuine PDF
generation and saving through the existing folder mechanism.

After deployment, a live operator must select the intended existing Dale folder,
confirm any native browser access prompt, verify its displayed name, click Save,
reload and verify the assignment, then test View Folder and Allow Access. Do not
create or move existing report directories for this check. If the native picker
still aborts before returning a handle, retain its exact new error text; do not
claim an IndexedDB repair, successful selection, or completed save.
