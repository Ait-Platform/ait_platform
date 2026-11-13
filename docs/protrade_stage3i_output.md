# ProTrade Stage 3I — Output

The central Output page lists only existing documents for the authenticated owner's selected shop. Supported sources are saved Carpentry quotations, existing mechanic quotation/billed documents and invoice records, recorded debtor-ledger statements, and existing trade-filtered Price Lists. No acceptance, conversion, ledger posting or new accounting behavior was added. Legacy unassigned records are preserved without guessing their shop. Setup remains accessible through Shop Setup.

Preview, Print and PDF export use the selected shop's profile and saved document/debtor details. Signed previews expire after 30 minutes and reject changed source data or recipient details. Actual invoice exports retain the existing job reference and stored totals; no invoice numbering system is introduced.

Saving reuses PM Folders in a separate `protrade` browser namespace. Select a base ProTrade folder, then reports go into `Shop-{id}-{name}/{document type}`. Existing filenames receive numbered copies through the shared PM writer. Handles remain browser-local. Folder access requires a supported secure browser and permission; other browsers receive uniquely named PDF downloads. Download initiation does not establish that the browser completed a save.

Email uses the existing AIT PDF mail helper, the linked debtor's current saved email address, and explicit confirmation. A unique database receipt is committed before attempting delivery. Repeated submissions of the same preview cannot resend it, including when delivery is uncertain. Check delivery before deliberately generating a fresh preview for another attempt. Transport acceptance does not prove inbox delivery.

## Production migration

Apply `protrade_output_007` before using Output email. Its predecessor is `protrade_debtor_006`. It creates only `protrade_output_email`; it does not update source documents, pricing or debtors. Destructive downgrade is intentionally disabled to retain duplicate-prevention receipts.

In the Render service environment, using its assigned production connection:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_output_007')"
```

Independently verify `current_database()` and `current_user` before applying it. Afterwards check the ProTrade Alembic revision and `SELECT to_regclass('protrade_output_email');`. Local migration succeeded against verified `ait_local_db`; production execution has not been verified.

## Validation

79 focused PostgreSQL ProTrade regression tests passed, including 13 Output tests. Checks cover owner/shop/debtor isolation, saved source preservation, signed/stale previews, actual PDF generation for all four document types, confirmation, durable duplicate blocking and mail/storage failure paths. Browser checks exercise the existing PM folder writer, nonoverwriting saves, separate shop folders, fallback downloads and print/email controls. All four rendered PDF samples were visually inspected. Email delivery was mocked; live SMTP delivery, production PDF rendering and user folder permissions still require deployment verification.
