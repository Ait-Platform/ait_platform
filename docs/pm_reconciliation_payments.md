# Metro Reconciliation - Record Payment

Record Payment sits beside Edit on each reconciliation row. The form captures
Payment Date, Amount Paid and Payment Reference. It derives the municipal
account and billing allocation period from the authorized reconciliation record,
not from the entered payment date or a browser-supplied account.

## Existing storage reused

BilMuniPayment / bil_muni_payment already stores independent account-linked
transactions with month, payment_date, amount and reference. Reuse it without
new tables or schema changes. bil_municipal_statement has an existing unique
(account_id, period) constraint, so account/month identifies the reconciliation
entry. Its existing edit flow prevents moving a statement to another period.
The Paid column remains the Decimal-safe sum of municipal payments explicitly
allocated to that account/month. Existing municipal payments still contribute.
Arrears, Balance and Due are never rewritten or reduced by recording payment.

## Safeguards

All GET and POST paths use existing property-manager scope. The target statement
is joined to its municipal account and checked against that property again.
CSRF, positive cent-precision amount validation, bounded references and valid
non-future dates apply. No gateway is called; this records an existing payment.

A transaction locks the property and statement, and takes a UUID advisory lock.
The existing bil_utilities_capture_audit stores a municipal_payment event with
actor/time, payment ID, target statement ID, account/period, payment fields,
before/after Paid totals, request UUID and canonical payload hash. An identical
retry returns the original payment; altered reuse is rejected. A separate form
with an already saved account/date/amount/reference combination is rejected,
including allocation to another period. Recurring references with different
dates or amounts remain possible. Payment and audit commit atomically.
No earlier payment or audit row is updated or deleted.

## Verification

Isolated local PostgreSQL tests cover multiple payments, account/period totals,
legacy payment preservation, property isolation, CSRF, invalid inputs, duplicate
retries, changed UUID payloads, duplicate transactions, audit-failure rollback
and a browser recording a payment then refreshing without duplication.
Independent PostgreSQL sessions verify simultaneous identical UUIDs and distinct
UUIDs for the same transaction cannot create duplicate payments. Existing
capture, reconciliation output, reading and filter regressions remain separate.
