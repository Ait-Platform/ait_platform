# Ordinary municipal METSOA generation

Generate METSOA uses /billing/utilities/<property_id>/generate-metsoa?billing_month=YYYY-MM.
Almond October review: /billing/utilities/26/generate-metsoa?billing_month=2026-10.

No migration is required. The feature reads existing property/account/meter records, bil_meter_reading, bil_municipal_statement and bil_muni_payment. Production pm_utilities_001 was confirmed applied by the owner in the preceding task. No production records were queried or modified in this implementation.

## Sources and period rules

The selected month determines the current reading: use the latest reading dated in that month and its immediately preceding distinct reading date, regardless of the previous reading's calendar month. Actual dates and elapsed days are retained. Identical duplicate records do not count as a second reading; conflicting values on either relevant date require review. Future readings are excluded. One reading, a decreasing/invalid counter, unsupported utility, conflicting account links or an unlinked meter are reported rather than estimated.

Account bill figures are read from the captured municipal statement for the selected account/month. They are not tariff-calculated, combined with legacy float totals, or overwritten. Paid uses only explicit municipal account/month allocations. Paid is not subtracted again from Balance/Due because the recorded bill figures may already include credits. Consolidated bill totals are withheld if any account's selected-month bill is absent. Insufficient meter data keeps the statement visibly incomplete even when bill totals are available.

The legacy layout's property heading, account summary and dated-reading table pattern are reused in an independent template; the legacy tariff, tenant/unit, Setup Wizard and bulk calculation paths are not invoked. No unverified utility charge rates or itemised charge amounts are invented. Authorised PM property managers can generate and view this statement without purchasing or consuming tokens. Authentication and property_scope checks remain mandatory. Existing legacy METSOA, checkout and token-controlled workflows retain their original requirements. No payment, token debit or unlock record is created by PM generation.

## Live verification

Deploy through existing Render workflow, then open the Almond October URL as the authorised property manager; no statement unlock or token balance is required for this PM route. Confirm account bill figures against Metro Reconciliation and payments against existing municipal allocations. Expected supplied differences: CEL888 31 kL; CXN998 1 kL; 7150696S 15 kWh; 2188802S 547 kWh. These were verified with isolated test records, not the live database. ANG489 requires a second genuine dated reading before consumption is available. Account-specific missing-information messages link to Metro Reconciliation with property 26 and month 2026-10 preserved. Existing data must be corrected only through the appropriate capture workflow; generation is read-only.

Meter Readings initially shows five newest records (date descending, then ID descending). Show More reveals all remaining rows and Show Less collapses to five, retaining edit links and selected billing month.
