# Effective-dated tariff compatibility

## Record audit

Owner-supplied read-only production audit confirms 13 records, unique codes within utility types, no missing effective dates and no existing historical versions. ELEC01 starts 2026-01-01; water, sewerage and fixed charges start 2026-06-05. A separate read-only inspection of verified local ait_local_db found the corresponding 13 identities with those dates. Production credentials were not available to the agent; production findings are attributed to the owner's query, not a new agent connection. No production records were changed.

## Rule

The shared read-only app/utils/tariff_versions.py resolver identifies tariffs by normalised utility type and code. For an explicit date it chooses the latest effective start not after that date; a YYYY-MM billing period uses its actual calendar month-end, consistent with property METSOA. In this existing start-only schema each distinct start supersedes the preceding version; no effective-to dates are invented or written. Duplicate applicable starts are conflicting overlapping versions and are rejected without a row-ID or price tiebreak. Missing, malformed and non-ISO dates block the affected identity. Future starts are excluded; future-only requested identities produce a clear unavailable error rather than a future price. Unrelated future-only identities do not enter current tier lists. Code-only lookups reject multiple utility identities. Multi-identity electricity fallbacks reject ambiguity rather than take an arbitrary first row.

No new end-date column, migration, inferred historical date or tariff-price write is introduced. Tariff editing remains unchanged and is not enabled by this work.

## Corrected consumers

| Path | Selection changes |
| --- | --- |
| app/admin/programs/billing/water.py | _tariff_latest_by_code and t_label now require the caller's billing month; calc_ws_sd_totals passes it. get_water_tiers and get_fixed_tariffs resolve one version before applying existing ordering, factors and formulae. |
| app/program_billing/routes.py | build_meter_charge_block, calculate_ws_sd_total, build_water_fee_block, build_electrical_rows, get_tariff_for_ws, get_tariff_for_sd and build_ws_sd_cost_block replace unbounded ORM reads with dated read-only adapters. Supplied-tariff branches used by property METSOA retain their formulas. |
| app/program_billing/metsoa_tariffs.py | Existing property selector delegates version grouping/date validation to the common resolver, preserving its validation, Decimal conversion, tariff classification and W_LOSS identity rule. |
| app/utils/billing_metsoa.py | _get_tariffs and _get_tariff_by_code resolve applicable versions. Electricity's existing explicit ElecRate preference remains; its fallback must be unambiguous. |
| app/utils/billing_map.py | _tariff_for_code_month and nested build_metsoa_rows lookups for code, electricity, tiers and mapped fixed charges use the common resolver. Electricity without the legacy ElecRate code may use the single verified electricity identity, including ELEC01; multiple identities are rejected. |
| app/utils/billing_metsoa_builder.py | _tariffs_by_code, get_tariffs_by_prefix, get_tariffs_for_ws, get_tariffs_for_sd and the mapped fixed-charge lookup select by billing month. Existing callers now pass that month. Water result shapes continue omitting a sewerage reduction factor where the old SELECT omitted it. |

App import/call inspection found no active tariff consumer from app/utils/billing_helpers.py: its only active app import is the unrelated ensure_recurring_materialized ledger helper. That file, its dormant historical tariff copies, and Ledger remain untouched. Re-enabling those copies would require explicit compatibility review. No legacy tariff-code aliases or new identities are inferred from similar descriptions.

## Factors 999

Stored S04 and W05 factors are unchanged. The property METSOA selector passes reduction_factor as the daily tier allocation multiplier; the existing breakdown builders multiply it by the reading-period day count and cap allocation at remaining consumption. In that path, 999 behaves as a large upper-tier allowance. Some older helpers apply their factor directly to quantity or cost. Schema naming and code alone do not establish the original municipal meaning of 999. There is no documentary evidence authorising a factor correction, so all values and arithmetic remain intact.

## Verification and limits

Focused PostgreSQL tests use session-temporary tariff/settings/map and statement-sentinel fixtures in verified ait_local_db. They cover current/historical/future prices, unknown dates including NULL, duplicate starts, distinct identities, fixed charges, ELEC01, water/sewerage boundaries and factors. SELECT-only capture and row snapshots prove that selection does not modify prices or stored statement amounts. Existing property METSOA HTML/PDF, surcharge, fixed insurance and Utilities regressions are run separately.

Source AST checks confirm that only the seven listed PM tariff-reading functions changed; all other PM route functions, including the Setup Wizard, are identical. Arithmetic expressions in those seven builders are unchanged. No persistence service, token handling, tariff UI, municipal reconciliation, meter readings, meter exceptions, lease, ledger or payment module is edited.

Saved statement rows and issued originals are not regenerated or written during verification. Existing explicit legacy regeneration/posting services are not activated or redesigned. Production migrations and production test transactions are not performed. Deployment and authenticated Render generation remain to be verified after push.
