# PM SOA opening balances and posting references

## Scope and inspected schema

The actual local PostgreSQL public.bil_tenant_ledger was inspected read-only before implementation. It has id (sequence default), tenant_id, txn_date, month, description varchar(255), kind varchar(50), amount numeric(10,2), ref varchar(255), and a non-unique tenant_id index. No opening-balance or reference uniqueness constraint existed. Production schema was not accessed.

The existing ledger and compatible BilTenantLedger ORM remain in use. No new ledger or account is created. Balances are calculated exclusively from signed Decimal ledger amounts, in transaction-date/ID order.

## Actions

- Opening Balance: one per registered tenant across properties and leases. Debit is positive, Credit negative, and Zero records an amount of 0.00. Description is always Opening Balance. An existing row with kind opening_balance or a case-insensitive, trimmed Opening Balance description also blocks a second opening. The button becomes disabled after saving.
- Post Charge: positive amount and the prescribed debit-description dropdown.
- Record Payment: negative amount and the prescribed credit-description dropdown. Refund paid is not offered as a credit. Refundable security deposits are outside this workflow.
- Optional explanatory notes are separate from the fixed/selected description, limited to 2000 characters.
- All forms use a server-assigned permanent D0001/C0001 reference. The field is read-only. Reference is displayed with the transaction description; the five ledger columns are unchanged.

Authentication, managed-property/registered-tenant association checks (including saved Lease Kit drafts), CSRF, signed scope-bound form keys, strict amount/date validation and retry-safe transactions remain in force. The account is tenant-wide: the existing ledger has no property/lease allocation column. Historical debts are not copied between tenants.

## Required migration and deployment order

Migration pm_soa_001 is a narrow independent Alembic branch for the existing ledger. Run the established database migration procedure targeting **pm_soa_001 only**, before enabling the updated posting service. Do not upgrade unrelated heads. No application-startup migration or automatic schema creation is introduced.

It adds three nullable metadata columns (posting_key, posting_hash, posting_notes), three unique indexes (form key, opening per tenant, D/C reference), and separate non-cycling PostgreSQL sequences limited to 1..9999. It initialises each sequence above existing exact D/C four-digit references. Historical rows and references are never updated or renumbered. Duplicate historical opening balances or D/C references block migration for review; it does not resolve them by altering financial records. Migration takes an exclusive ledger lock during preparation.

Before writes, the service verifies required columns/indexes and READ COMMITTED isolation. If migration is absent, posting fails closed with an explicit migration message; existing read-only ledger viewing remains supported. Migration has been exercised only on disposable local PostgreSQL fixtures, not applied to production or the local public application ledger.

Reference numbers may have gaps because PostgreSQL sequences are not rolled back. At 9999, the affected sequence stops with a clear error instead of wrapping or generating a fifth digit. A zero opening marker uses the debit sequence.

## Transaction and history protection

Signed form nonces bind manager, property, tenant and action. A transaction advisory lock serialises retries; a unique posting_key index adds persistent database protection. A payload hash rejects reuse of a form key with changed details. Prior-release PMPOST references retain their duplicate-protection check and are left unchanged.

Opening creation takes a tenant-wide advisory lock and is enforced by a unique partial database index, including concurrent writers that do not cooperate with the advisory lock. Independent D/C sequences are concurrency-safe and exact D/C references are protected by a unique partial index.

The existing ORM inserts the financial row. Only that just-created row receives posting metadata in the same transaction. Existing ledger rows are not edited. Financial posting, migration row preservation, dropdowns, access restrictions, real multi-connection reference allocation, opening uniqueness and retry constraints are tested locally.

No recurring rent, METSOA transfer, payment allocation, deposit accounting, deletion, historical editing, ProShop synchronisation or unrelated lease changes are included.
