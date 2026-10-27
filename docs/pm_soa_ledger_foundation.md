# PM SOA manual charges and payments

The account remains the existing registered bil_tenant ID and Tenant Reference. No new account, ledger, migration or reference is allocated.

## Existing financial foundation

- bil_tenant_ledger is reused for signed rand amounts: positive manual charges and negative payments. Reading remains chronological by txn_date, then ID, with Decimal running balances.
- Existing ORM BilTenantLedger matches the inspected local PostgreSQL columns: id (sequence default), tenant_id, txn_date, month, description varchar(255), kind varchar(50), amount numeric(10,2), ref varchar(255). The shared posting service uses that ORM mapping for both inserts.
- bil_lease and saved bil_lease_draft selections supply existing property/tenant associations. Production's verified sectional_unit_id relationship is supported; bil_lease.property_id is not assumed.
- bil_tenant_recurring, rental helpers and METSOA routines are not invoked. Municipal-account payments, manager/platform statement fees and shared Debtor/ProShop accounting remain separate.
- Refundable deposits have no verified separate liability workflow in this ledger; no deposit action is provided.

## Posting safeguards

The POST-only PM endpoint revalidates manager ownership, the selected property and registered tenant association using the same PM resolver as the property dropdown. Property, tenant and relevant draft rows are locked during the transaction. Existing authentication and CSRF protection apply.

Both actions use one service, Decimal amounts, strict calendar dates, required descriptions and a maximum of R99,999,999.99. Payment references are required; charge references are optional. User references are limited to 100 characters.

A signed form key binds the nonce to the manager, property, tenant and action. The existing ref field stores PMPOST:<nonce>:<payload SHA-256> | <user reference>. PostgreSQL READ COMMITTED isolation is required so a waiting retry sees the preceding commit. A PostgreSQL transaction advisory lock serialises checks and inserts for that nonce across workers. Identical retries return success without another row; changed-payload reuse returns a conflict with input preserved. No session-only duplicate cache or schema change is used.

The live catalog is checked before writing. Missing required columns, inadequate amount/ref capacity, unsupported mandatory columns or a missing automatic ID cause posting to fail closed. Local public schema was inspected read-only; live Render schema and production posting were not exercised.

## Boundaries

The ledger is tenant-wide: the existing table has no property/lease allocation column. Selecting a property authorises access to that tenant account; this feature does not attribute historical rows to a tenancy, transfer balances or rewrite history. POST appends rows only.

No automatic rent, recurring materialisation, METSOA transfer, allocation, deposit posting, deletion, historical editing or ProShop synchronisation is enabled. Existing financial rows and lease documents remain unchanged.
