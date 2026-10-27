# PM SOA read-only ledger foundation

The PM account is the registered bil_tenant ID and its existing Tenant Reference. No new account or reference is allocated by viewing SOA.

## Existing foundation inspected

- bil_tenant_ledger: tenant_id, txn_date, description, kind, signed amount (Numeric rand), ref and month. Existing PM SOA sums signed amounts; charge rows are positive, payment rows negative, and reversals/adjustments retain their signed amounts. Reused for reading only, ordered by timestamp and ID, using Decimal balances.
- bil_tenant_recurring and bil_rent_schedule: recurring/rental inputs used by existing posting helpers. The actual local PostgreSQL catalog contains bil_tenant_recurring; bil_rent_schedule was not present. Not called or materialised by this page.
- bil_lease: rental and arrears particulars; legacy production links through sectional_unit_id. Lease Kit draft JSON stores current saved lease particulars and selected tenant IDs. Existing SOA association reader remains authoritative for access.
- bil_metsoa_tenant_month and bil_metsoa_meter_month: existing tenant/meter monthly municipal calculations. Existing METSOA posting writes PM ledger charges and may synchronise to shared debtors; none of those routines is called.
- bil_property_payment_account, bil_property_utility_arrangement and bil_bank_detail: payment destinations and recoverable-utility particulars, not tenant receipt transactions.
- bil_muni_payment: payments to municipal accounts, not tenant receipts. bil_municipal_payment includes municipal deposit particulars; it is not a tenant refundable-deposit subledger.
- bil_statement_payment: manager/platform statement-unlock payments; not tenant rent receipts. bil_statement is a tenant statement relationship, not a complete account transaction source.
- Refundable deposit amount is present in saved lease particulars (_amounts.deposit_amount). No verified separate tenant deposit-liability accounting classification was found in the inspected PM ledger model. The viewer does not classify deposits as rental income.

## Reuse boundaries

The existing admin tenant SOA reads bil_tenant_ledger. Its running-balance convention is reused, without its float arithmetic. The admin ledger GET and rental/recurring helpers can post transactions; those routes are not reused. The legacy /manager/tenant_ledger route uses shared Debtor records; it remains unchanged and is not linked from PM SOA.

The new GET-only PM ledger checks the selected managed property, resolves the registered tenant through the existing PM association reader, then reads that tenant ID only. No debts are copied, reallocated or inferred from property/lease amounts. Missing schema or invalid amounts produces an explicit service error rather than sample/zero transactions. Unknown dates are displayed as unrecorded and ordered last.

## Next stage

No posting forms, scheduled charges, payment entry, adjustments, statement export, deposit receipt/refund workflow or transaction reclassification is enabled here. Before posting: verify live PM ledger schema/data and currency/sign conventions, reconcile legacy opening balances, define separate refundable-deposit liabilities, confirm recoverable municipal allocations, and establish tenant/tenancy attribution (the existing ledger has tenant_id but no verified property/lease allocation column). Existing transactions remain unchanged.
