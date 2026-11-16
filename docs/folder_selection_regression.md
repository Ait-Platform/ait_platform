# PM / ProTrade folder selection investigation — 16 November 2026

## Evidence and limits

- User confirmed that choosing existing local D: folders returns a native `showDirectoryPicker` AbortError in both programmes. ProTrade's catch is immediately around the picker; no permission request, IndexedDB write or subfolder creation has run at that point. This proves the observed rejection precedes AIT persistence; the underlying reason is not established by that error alone. An AbortError does not prove that the user cancelled.
- Compared PM at `92b6a3ef` with `134c92c9`: the picker invocation, scope key, database `ait-pm-directories`, store `assignments`, handle record and transaction completion handling are unchanged. PM controller changes between those commits are success-message wording only. Later shared-library changes remove eager directory creation and add lazy PT layout/history; they do not precede the picker.
- No Git commit has been independently established as the last physically verified Windows run. The old PDFs establish earlier successful saves, not which browser/code version created them.
- Two confirmed application defects: PM displayed the selected name only after permission/storage succeeded and discarded the in-memory handle on failure; its catch suppressed **all** AbortErrors, including IndexedDB transaction failures. Reload did not automatically restore the displayed PM selection. Permission renewal also fetched IndexedDB before requesting permission, risking loss of the required user gesture.
- Installed Chrome executable was inspected read-only: `157.0.8103.52`. Chromium issue [582407452](https://issues.chromium.org/issues/582407452) describes the same Windows rejection, introduced in 157 and verified fixed in 158. This is supporting evidence, **not proof** that this particular browser failure has the same cause. No browser security settings, permission barriers or policies were changed.
- Live Windows observation during this investigation: the localhost diagnostic's current read-only call returned **SUCCESS** with handle name `METSOA Statement`. The operator selected the folder manually. This disproves a blanket failure of the installed Chrome directory API. It does not establish the same base-folder selection on the AIT origin or prove end-to-end persistence. The earlier blanket Chrome attribution is withdrawn. Results of both calls using the same base folder are still required to isolate origin/location-specific behavior.

## Focused correction

Both programmes now separate selection, permission and storage errors. PM displays the returned directory immediately, retains it for retry, and adds explicit Save using the existing IndexedDB API. Its existing Allow Access control renews the displayed handle directly from the click gesture. Saved PM selection restores on refresh. PT retains its existing staged workflow with more precise error reporting. A picker AbortError is reported neutrally because cancellation, permission refusal and browser failure cannot reliably be distinguished from its name.

Existing IndexedDB keys/records, PM `source / report` folders, PT owned `user / shop / type` folders, lazy creation, saved files and filenames are unchanged. Uploads, renderers, financial calculations, database migrations and memory settings are outside this patch.

## Verification

Automated Chromium tests use real OPFS DirectoryHandles and IndexedDB with a substituted chooser. They exercise selected-name display before storage, explicit Save, refresh restoration, user/property isolation, permission denial/retry, picker AbortError/denial, failed IndexedDB open/write, preservation of old assignments, and retry with the pending handle. Existing PM/PT regressions cover historical PDF bytes/viewing, unchanged folder layout, lazy creation, collisions, failed writes and document saving.

These tests **do not establish that the native Windows picker is fixed**.

Final focused run: **50 tests passed, 14 subtests passed, no failures or skips** across `test_folder_selection_stages`, `test_pm_directory_register`, `test_pm_directory_layout`, `test_protrade_dashboard_support`, `test_pm_output_local_save`, `test_pm_output_pdf_failures`, `test_pm_output_save_period`, `test_protrade_output`, and `test_protrade_boq_folders`. Database-backed fixtures were explicitly restricted to localhost `ait_local_db` and temporary tables. A legacy PDF-failure assertion was corrected to require no destination directory after failed generation, consistent with lazy creation.

For a controlled live comparison, serve `tests/manual` on loopback (without starting the Flask application):

```powershell
.\.venv\Scripts\python.exe -m http.server 8766 --bind 127.0.0.1 --directory tests/manual
```

Open `http://127.0.0.1:8766/folder_picker.html` in the affected Chrome. Test both buttons with the same existing base folder and approve any prompt manually. Neither button persists anything or writes a file. If both fail before storage, rolling back AIT persistence cannot resolve that failure. Compare the same page in Edge or an updated Chrome without disabling security features.

After deploying the commit, live acceptance still requires **both** actual PM and PT pages: choose their existing folders, check the name, Save, handle permissions manually, reload and check restoration; retrieve an existing METSOA PDF without regenerating it; save an authorised document and confirm its actual file. Do not overwrite or relocate historical PDFs. No migration is required.

## Post-selection follow-up

The standalone page only reads `handle.name`; it neither retains a pending selection for Save nor uses IndexedDB. In the actual PM/PT controllers, the returned handle is assigned to their closure's `selected` variable before displaying the name. Save calls `requestPermission` directly from the click, then `AITDirectories.set`. The shared store resolves only on transaction completion, using the unchanged user/property key and persisted DirectoryHandle. Page initialization retrieves that record and restores the displayed name. A later read cannot overwrite a newly selected handle. Tests cover persistence and failure/retry with real browser storage.

An additional, reproducible post-selection defect was found: `AITDirectories.set` committed IndexedDB and then called `notify`; an exception in optional `BroadcastChannel` creation/messaging could reject the overall Save **after the folder was already stored**, producing incorrect failure feedback. A focused browser test reproduced the stored record alongside a failure message. Cross-tab notification errors are now isolated from the committed Save; the same-tab update event still runs. This is a confirmed application defect, not proof of the cause of the originally reported picker AbortError. No further Chrome compatibility investigation is part of this follow-up.
