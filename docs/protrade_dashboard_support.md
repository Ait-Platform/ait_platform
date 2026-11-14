# ProTrade dashboard: Folders and Help

No database migration. No accounting, pricing, token or mechanic behavior changes.

Folders and Help are authenticated read-only routes, scoped to the signed-in
owner and optional active shop. The dashboard retains its existing menu links.
Help covers the nine current workflow topics and returns to the same dashboard.

Folders reuses Output's folder controls, JavaScript and AITDirectories API.
The existing IndexedDB key remains [user, protrade, __main__]; no settings are
moved, reset or duplicated. The manager prepares Shop / Report Type folders for
owned shops. Output continues using the same paths, Save Report, collision
protection and Download PDF fallback. Permissions remain browser controlled.

Verification uses temporary PostgreSQL tables, actual routes/templates and
Playwright. Native serializable browser directory handles and real IndexedDB
verify persistence across reloads and the Folders-to-Output transition. The
headless chooser supplies a browser test folder; denied permission is simulated
without replacing the saved assignment. Saving two shops' PDFs verifies distinct
shop/report paths. Existing tests cover repeat filenames, downloads, print,
email confirmation, authentication/isolation and accounting regressions.

Production OS folder selection and browser permission prompts still require the
user's browser consent. No production data or browser settings were changed by
the automated checks.
