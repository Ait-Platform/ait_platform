# ProTrade release: ordinary PDF downloads

ProTrade Output keeps the existing owner-scoped shop selector, document catalogue, preview, print, email confirmation and signed PDF endpoint. The heading is ProTrade Output. Download PDF uses the browser's normal attachment download; no directory picker, local permission or browser file-history dependency is loaded. Shop Setup is removed from Output; the dashboard shop-name link still opens the existing Shop Profile.

The ProTrade menu no longer offers Folders. Authenticated legacy folder URLs validate shop ownership, then redirect to Output. Paid BOQ sections keep their existing download endpoint and stop loading local-folder scripts. Help and statement links describe the download workflow.

Existing application-backed quotations, invoices, BOQs and saved SOAs remain available. Previously downloaded files remain on the user's device; this change does not upload or discover arbitrary local files. PM's JavaScript, templates, IndexedDB assignments and METSOA files are unchanged. There is no database migration, renderer change, upload change, memory-setting change or financial calculation change.

Verification: 24 focused tests and 7 subtests passed, no failures/skips, including actual browser downloads, print invocation, debtor email confirmation, authorised shop switching, stored document access, unchanged ledger values, paid BOQ downloads without another deduction, PM selection/Save/refresh, and permission/storage failure handling.

Deploy the commit through the normal Render service release. No migration command is required. After deployment, verify one real shop switch, quotation/BOQ/invoice/saved-SOA preview, Download PDF and Print. Native printer completion and the final production browser experience still require the user's live check.
