# Carpentry quotation ledger integration

Quotation Register now shows Quote No., Date and Amount. Stored draft/status values and all existing quotation/item routes and numbering are unchanged.

## Existing ledger, one source charge

`debtor_ledger.boq_quote_id` references the existing BOQ quotation and is unique. PostgreSQL posts one debit for each saved, debtor-linked Carpentry quotation (including saved drafts), in the same transaction as quote creation/editing. Full-editor and individual-item saves use the same database trigger. Corrections update the linked debit's amount in place, retaining its ledger ID, original transaction date and history position. They do not create an invoice, new quotation or token transaction. Only quotation financial/identity updates run the synchronization; status-only changes do not repost charges.

The guard validates quote/debtor/shop/owner agreement, protects linked charges against manual amount changes, unlinking and deletion, and prevents moving a charged quote to another account. The existing ledger table is reused. Amount storage is widened from 32-bit integer to exact NUMERIC(28,0) integer cents, matching the existing BOQ totals' range. Its ORM type still returns Python integers to preserve existing mechanic/billing arithmetic. Carpentry ledger/SOA formatting avoids float conversion for large existing amounts.

## Existing payments and SOA

The existing account page records Carpentry payments as credits with date, amount, reference and a generated idempotency key. A unique debtor/key and a unique debtor/date/amount/reference fingerprint protect multiple Render workers and repeated submissions. The same reference on a different date is allowed. Different data with a previously used key is rejected. Existing unkeyed payments are retained; identical existing credit/date/amount/reference submissions do not create another credit. Carpentry payment capture does not run the legacy mechanic job-status/invoice path.

New shop debtors have an explicit zero opening balance; the database default is zero. Existing balances and transactions are not reset. Running balances use chronological transactions with date/ID tie breaking; filtered periods carry prior transactions forward and exclude transactions after the end date. The existing SOA preview, PDF and email-rendering paths use that same account history and the registered Carpentry shop's details and bank text. No automatic email is sent. Existing interest/recurring-charge preferences and non-Carpentry workflows are not changed.

## Required migration

Revision: `protrade_quote_ledger_009`; parent: `protrade_quote_numbers_008`; no extra dependency or branch merge.

It adds the nullable source/payment fields, precision, constraints and synchronization/guard triggers. In one transaction it backfills only permanently linked Carpentry quotations with a matching shop/owner/debtor. Unassigned legacy quotes remain intact rather than being guessed into an account. A single existing debit with the exact quote reference and matching amount is linked without changing its original values. Multiple matching debits or a mismatched amount abort the migration for reconciliation; nothing is deleted, merged or silently doubled. Existing original quote, invoice, debtor, catalog, Price List and ledger values are preserved. Table writes are briefly locked while backfill and triggers are installed.

Use the running Render service's assigned DATABASE_URL. Independently verify current_database() and current_user before production writes, then run this targeted command from the repository root (no application factory/startup maintenance):

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_quote_ledger_009')"
```

Do not stamp the revision, run a destructive downgrade or manually insert additional quote charges. Standard Alembic tracking prevents an already-applied migration from being reapplied. Verify the revision and linked debits before testing the live ledger. A downgrade is intentionally blocked to preserve accounting links/history.

## Verification

Verified local identity was ait_local_db / ait_local. The local migration succeeded; before/after comparisons confirmed every original quotation, invoice, debtor, Price List, catalog and original ledger value was unchanged. No eligible linked quotes existed in that local persistent database; existing-quote backfill is covered by isolated PostgreSQL fixtures. Production migration and live verification are pending.

Focused PostgreSQL/browser tests cover zero-start debtors, Q-0005 R1800 plus Q-0006 R1473.32 = R3273.32, a single R500 credit = R2773.32, same-record edits, repeated saves/payments, legitimate reference reuse, exact large cents, source guards, rollback, ambiguous backfill safety, shop/owner/debtor isolation and unchanged tokens/pricing/invoices. Concurrent tests send 20 quote updates and 12 duplicate payment inserts and retain one debit/one credit. The real loopback HTTP browser follows Debtor Register -> Ledger -> Payment -> SOA -> quote item correction; SOA PDF output is rendered and visually checked. Existing mechanic, quotation-numbering, item-editor and Output regressions are included.
