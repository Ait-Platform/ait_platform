# AIT repository instructions

Read and follow [AGENT.md](AGENT.md) for the established repository, application, database and UI rules. The convention below supplements those instructions; it does not authorise production migrations or financial changes.

## ProTrade release exception

ProTrade uses ordinary PDF downloads and application-backed document selection. Do not reintroduce local folder controls, permissions or browser file history into ProTrade. Keep the shared local-folder APIs and Property Management behavior unchanged. Existing local PDFs remain on the user?s device.

## Reusable browser folder convention

**PM clean rebuild exception (20 November 2026):** The replacement PM module is
`pm_property_folders.js`, with its own new database and no legacy picker/storage
reuse. It remains disconnected from production until native browser selection is
verified. The Folders page must not load the retired picker. Preserve legacy
assignments and existing report saving during this gate; do not migrate, delete,
or silently reuse old assignments. The shared conventions below continue to apply
to existing adapters, not as authorisation to reconnect the retired PM picker.

- The user chooses a base folder using **Select Folder** and grants browser access. PM and ProTrade display the selected name, then use **Save** to request/confirm access and persist the handle; its saved-document list refreshes automatically without background permission prompts. Property Management retains **Allow Access** to renew permission for its displayed handle and its established View Folder controls. Do not create document directories during selection, permission renewal, navigation or historical-file listing.
- Reuse `app/static/js/pm_local_directories.js` (`window.AITDirectories`) rather than implementing another folder-handle store. Preserve the existing IndexedDB assignments and their user/client scope. Server-produced folder/report configuration must contain only businesses/properties authorised for the current user. Never accept a filesystem path or another user's folder assignment from an HTTP request.
- New programme adapters supply stable ownership IDs and safe display names for `base / user / client / document type`. Create only the required destination when a document is actually saved. Handle identical display names without merging accounts or overwriting files. ProTrade supplies a three-component `path` and `owner` descriptor; local identity markers retain original paths after name changes. Do not hard-code a person's name, business name or drive.
- Preserve established programme layouts. Property Management's existing per-user/property base-folder assignments and `source / report` structure are a compatibility adapter: retain those paths and filenames, and create the requested subfolders lazily. Do not move, wrap, rename, delete or migrate its existing directories to impose another programme's layout without separate authorisation. For new programmes, use the user/client/document convention above.
- Keep directory handles, ownership markers and local file bytes on the user's device. Do not send local paths/handles/files to AIT unless a separate upload feature is explicitly requested. Namespace assignments by AIT user and authorised client/property; resolve ownership using stable IDs, not display names alone.
- Fetch and validate the PDF before creating its destination. Use an origin-wide lock and collision-safe filenames. Retain existing files and names. Declare **Saved** only after the writer has successfully written and closed; surface permission, renderer, storage and write failures clearly. A browser download being started is not proof that the download completed.
- Reuse `templates/partials/local_document_history.html` and `app/static/js/document_folder_history.js` for historical PDF retrieval and native viewing/printing. Historical viewing must read the existing bytes; do not regenerate documents, repeat token deductions or rewrite financial snapshots. List/read operations must not create directories. Keep legacy-path retrieval where layouts have evolved.
- Feature-detect secure-context File System Access, IndexedDB and required Web Locks. Explain unsupported browsers and expired permissions, and retain the established Download PDF fallback. Native PDF viewers provide their own Print control; do not claim printing completed.
- Test lazy creation, permission denial/renewal, unsupported-browser fallback, same-named client separation, assignment isolation, old-file retrieval, failed writes and filename collisions. For shared API changes, run relevant Property Management and ProTrade browser regressions.
