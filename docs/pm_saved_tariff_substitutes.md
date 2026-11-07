# Saved historical current-rate substitutes

Scope: local implementation and isolated tests only, as requested on 7 November 2026. No production access, tariff writes, meter-link changes or migration were performed. The actual local tariff database and existing readings remain unchanged.

## Explicit operation

The existing Admin tariff service now exposes `fill_missing_historical_substitutes(month)` for an explicitly authorised Admin request context. It is not invoked by a report, scheduler or new route. For February 2026 the target start is **2025-07-01**. It copies only currently available water/sewerage families absent from that reporting period. It never copies electricity or replaces existing historical evidence. Existing Admin Save supplies locking, optimistic version checks, numeric validation, insert-only versions and actor/timestamp evidence. Future metadata changes, malformed records and ambiguous selections are rejected rather than guessed. Every version is committed through the established Save operation; a retry skips committed copies, including after a partial failure.

The existing `bil_tariff_verification.source_reference` field stores a structured substitute marker, original tariff ID/date, target start date and copy date. No schema change is needed. Missing audit storage stops the operation; it is not silently created. No production invocation was attempted. Production execution still requires the approved connection and an authenticated Admin context.

## Report classification

Persisted substitutes are recognised without the override checkbox. They remain **ESTIMATED**, even though their copied effective date covers February. Available-rate coverage removes **INCOMPLETE** only when no genuine blocker remains. The concise notice identifies the save date and explicitly says the copied rates are not verified historical tariffs. Original source dates remain visible in component disclosures. Invalid provenance leaves a report incomplete rather than implying verified rates.

The calculator, tiers, allocations, report routes and compact PDF layout remain unchanged. Existing historical rates and the verified 3.2795 electricity rate are preserved. No replacement bulk-link audit evidence is fabricated.

## Local integration calculation

The temporary 13-meter fixture reproduces the supplied February consumption values and explicitly saves **five confirmed children** of bulk ANG489. The unlinked zero-consumption water meter remains registered and keeps its per-meter levy. Bulk usage 82 kL minus five linked individual usages totalling 85 kL remains an informational **-3 kL** difference; the existing pricing floor produces no refund.

Copied fixture rates are W01-W05: 39.39, 46.70, 62.17, 95.91, 105.39; S01-S04: 5.45, 9.20, 17.54, 27.38; W_LOSS: 19.30; W_MGT: 144.54 (the existing flat total representing 6 x 24.09); S_REFUSE: 206.93; and independently available water/sewerage surcharges of 1.48/kL. These are test source values, not a claim about current production rates.

* Electricity: **5935.89** at 3.2795/kWh.
* Water/sewerage, including both available surcharge fixtures: **5904.33**.
* Result: **11840.22 ? ESTIMATED**, with management/refuse each once on ANG489, seven per-meter loss levies, no INCOMPLETE banner, and no automatic refund.
* When no sewerage surcharge is configured, none is invented: the isolated estimate is **11714.42**. The difference is 85 kL x 1.48 = 125.80.
* If a required water surcharge is missing, the report remains **ESTIMATED (INCOMPLETE)** and names that component.

The February fixture PDF was regenerated and both pages visually reviewed under `tmp/saved-tariff-substitutes`. These totals are fixture results, not a regenerated production Property 26 statement. Production rates/readings/confirmed links were not accessed or changed.
