# Metro Reconciliation - Record Payment

Record Payment sits beside Edit on each reconciliation row. The form captures
Payment Date and Amount Paid; Payment Reference is generated automatically.
It derives the municipal account and billing allocation period from the authorized reconciliation record,
not from the entered payment date or a browser-supplied account.

## Existing storage reused

BilMuniPayment / bil_muni_payment already stores independent account-linked
transactions with month, payment_date, amount and reference. Reuse it without
new tables. The additive sequence migration described below is required. bil_municipal_statement has an existing unique
(account_id, period) constraint, so account/month identifies the reconciliation
entry. Its existing edit flow prevents moving a statement to another period.
The Paid column remains the Decimal-safe sum of municipal payments explicitly
allocated to that account/month. Existing municipal payments still contribute.
Arrears, Balance and Due are never rewritten or reduced by recording payment.

## Safeguards

All GET and POST paths use existing property-manager scope. The target statement
is joined to its municipal account and checked against that property again.
CSRF, positive cent-precision amount validation, valid non-future dates apply. References are generated on the server. No gateway is called; this records an existing payment.

A transaction locks the property and statement, and takes a UUID advisory lock.
The existing bil_utilities_capture_audit stores a municipal_payment event with
actor/time, payment ID, target statement ID, account/period, payment fields,
before/after Paid totals, request UUID and canonical payload hash. Generated
references are stored in both the payment and the new audit event. An identical
retry returns the original payment; altered reuse is rejected. A separate form
with an already saved account/date/amount combination is rejected,
including allocation to another period. Payments with different dates or amounts
remain possible and receive separate references. Payment and audit commit atomically.
No earlier payment or audit row is updated or deleted.

## Verification

Isolated local PostgreSQL tests cover multiple payments, account/period totals,
legacy payment preservation, property isolation, CSRF, invalid inputs, duplicate
retries, changed UUID payloads, duplicate transactions, audit-failure rollback
and a browser recording a payment then refreshing without duplication.
Independent PostgreSQL sessions verify simultaneous identical UUIDs and distinct
UUIDs for the same transaction cannot create duplicate payments. Existing
capture, reconciliation output, reading and filter regressions remain separate.

## Sequential reference migration

Revision pm_mar_payment_reference_001 follows pm_meter_reference_001 and adds
only PostgreSQL sequence bil_mar_payment_reference_seq. It does not rewrite
financial rows, old references or audit history. The initial allocation follows
existing three-digit references; unrelated legacy account-number references do
not inflate the starting number. Runtime allocation skips an exact historical
reference collision. PostgreSQL nextval provides atomic allocation across
properties and sessions, with no cycling or resets.

References have minimum width three (001, 002, ... 999, 1000). Rollbacks can leave
gaps; allocated numbers are never reused. A saved request replay returns the
same payment without allocating another number. Older audit receipt hashes are
accepted when their unchanged original payload matches. A payment-history table
under Record Payment displays all saved references for the selected entry.

Apply the migration through the separately approved production migration
procedure before recording new MAR payments:

    python -m alembic upgrade pm_mar_payment_reference_001

No production migration or payment-data changes were executed during development.
Until the sequence is present, saving new payments is safely disabled. Existing
history and Paid totals remain available.
