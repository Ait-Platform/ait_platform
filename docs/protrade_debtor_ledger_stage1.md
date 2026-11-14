# ProTrade debtor account connection ? Stage 1

Debtor Register now links Quote and Ledger to the selected saved debtor. Ledger reuses `/mechanic/client_ledger/<debtor_id>`, with the selected shop checked against the debtor and owner. Back returns to that shop register. Job-card lookup and the existing payment status update use the saved shop/debtor IDs for linked debtors; legacy unassigned accounts retain their previous behaviour.

Existing accounting infrastructure inspected:

- `debtor`: permanent owner/shop/customer account identity.
- `debtor_ledger`: dated debit/credit entries, amounts stored as integer cents, references and descriptions. Existing account page calculates running balance.
- `/mechanic/client_ledger/<debtor_id>/add_payment`: existing transaction capture, including legacy job-status updates.
- `/debtor/<debtor_id>/soa` in `app/program_debtors/routes.py`: owner check, date-filtered account statement, PDF output and existing email workflow.
- `debtor_charge_map`: existing recurring-charge definitions; no new posting mechanism added.

Deferred for confirmation before a later accounting stage: payment replay protection (no unique submission key in the ledger), invoice posting identity, immutable correction/audit rules, opening balances in filtered ledger views, and shop-specific SOA branding/default sender selection. The existing SOA path may fall back to owner-level sender/bank defaults; this stage does not certify new trade-specific statements. No four-button accounting hub, new tables, new posting workflow or migration was introduced.

Quotation editor: Register uses Edit; scoped hidden-area CSS prevents styles from leaving hidden areas visible. Existing saved snapshot loading and same-record updates are reused. Local PostgreSQL/Chromium tests reproduce Tomas at R1,149, edit to R766, save and reopen without another record or changed identifiers. This is a test fixture, not a production modification.
