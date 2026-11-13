# ProTrade short quotation numbers

Quotation display references are Q-0001, Q-0002, and so on independently for each shop. Width is a minimum of four digits, so Q-10000 is displayed in full. Carpentry quotations and mechanic job-card quotation histories share the shop sequence, including when a shop's trade changes. The existing Carpentry quote_number UUID, database IDs, job_number values, URLs, quote keys and invoice identifiers remain intact.

The migration backfills assigned-shop documents chronologically by created_at, with missing dates last and deterministic document-kind/ID tie breakers. Mechanic job cards retain their original invoice/job reference; the new number identifies the quotation represented by the same job record. Unassigned historical mechanic records are preserved without guessing their shop; their original references remain until they are explicitly linked to a shop.

PostgreSQL triggers allocate the number inside the document save transaction using a shop counter row. A central allocation registry and per-table uniqueness constraints prevent duplicates across both kinds of quotation. Concurrent requests serialize through the shop counter. Failed transactions roll back allocation. Edits and repeated submissions retain the existing number. Deleted documents do not release a committed number for reuse. Assigned shop, ID and sequence cannot be silently changed. ORM mappings fetch trigger-generated values on flush.

Carpentry forms/registers, quotation Output/PDF references, mechanic quotation forms and quotation documents use the short display reference. Invoice rendering, accounting references and invoice numbering retain job_number. The UUID remains available internally for duplicate-save protection and existing links; it is not the visible quotation number. The existing numbering fallback preserves older/unassigned records before migration; apply the migration before releasing the updated UI.

## Production migration

Required revision: protrade_quote_numbers_008, following protrade_output_007. It adds quotation_seq to protrade_boq_quotes and mech_job_cards, creates protrade_quote_counter/protrade_quote_number, backfills the numbers and installs allocation triggers. It does not update any original data columns. Existing source tables are exclusively locked briefly during the backfill/trigger installation so concurrent writes cannot bypass it. Run in a suitable deployment window before enabling updated application code.

Use the running Render service's assigned PostgreSQL connection. Independently verify current_database(), current_user and the intended schema first, then run the targeted migration:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_quote_numbers_008')"
```

Verify the Alembic revision, populated quotation_seq values for shop-linked records, and the two protrade_quote_number triggers. Normal application writes must retain access to the counter and registry through the migration's security-invoker trigger function. Destructive downgrade is disabled because references are permanent. Do not reset counters or delete allocation records as a rollback procedure.

Local upgrade succeeded on verified ait_local_db. Hash comparisons confirmed every original quotation, invoice, debtor and price-list column was unchanged. Production execution and live rendering remain unverified.

## Verification

Focused PostgreSQL/browser/Output tests cover chronological backfill, original-column preservation, independent shop sequences, simultaneous mixed quotation creation, rollback, no reuse after deletion, immutable numbers, repeated saves and edits, ORM flush behavior, short visible register/form references, an actual generated customer PDF and unchanged invoice references. The parallel test creates 20 mixed records and verifies exactly numbers 1 through 20. The customer quotation PDF was rendered and visually checked. Existing quotation input, pricing, owner/debtor isolation, mechanic and Output regressions are included. No live email was sent.
