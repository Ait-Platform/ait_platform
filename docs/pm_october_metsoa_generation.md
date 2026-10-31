# Original tariff METSOA reconnection

The existing PM Generate METSOA URL remains /billing/utilities/<property_id>/generate-metsoa?billing_month=YYYY-MM. Add &format=pdf for an inline PDF generated from the same original metsoa.html template. Property access and the PM token-free policy are unchanged. Legacy statement/token routes are unchanged.

## Sources

The PM adapter reads registered property meters and municipal account pointers, canonical bil_meter_reading pairs, bil_tariff and recorded municipal owner names. It does not read Metro Reconciliation statement figures or payments, and never reads/writes historical bil_consumption for generation. It never invokes generate_consumption_records_from_readings or any posting function.

Existing build_electrical_rows, build_water_rows and water/sanitation detail builders accept optional scoped in-memory records and explicit tariffs. Their legacy callers keep their original defaults. PM uses Decimal values and effective database records; no hard-coded surcharge, water-loss, management or refuse fallback is used. The original electricity summary, water summary, side-by-side water/sanitation band details and final-total position are retained. PM rates are printed without the original one-decimal water-rate rounding. Missing inputs produce a prominent INCOMPLETE marker and withhold the final amount due; any displayed figures are confirmed components only. Summary and tariff details print on separate pages.

## Effective dates and discrepancies

For the selected billing month, the statement date is its last day. Choose the latest effective record per utility/code on or before that date; future versions are excluded. Duplicate latest versions, multiple electricity tariffs, invalid rates/dates/band metadata, insufficient band coverage, or tariff changes crossing a reading period require review rather than guessed allocation. Actual reading dates are retained.

The owner reported 13 live tariffs: ELEC01 R3.2795 effective 2026-01-01; W01-W05 R39.39/46.70/62.17/95.91/105.39; S01-S04 R5.45/9.20/17.54/27.38; W_LOSS R19.30, W_MGT R144.54 and S_REFUSE R206.93 effective 2026-06-05. These effective dates precede October 2026. Full live band/unit/daily-allocation metadata was not supplied in that report and remains subject to runtime validation and owner confirmation against the approved tariff schedule.

The report contains no explicit WSSurcharge/SDSurcharge rates. Original builders hard-code R1.48 for both. PM does not silently assume that rate, or assume zero instead. Surcharge applicability/rates must be confirmed and explicitly recorded before a final amount can be produced. Historical PDF example rates differ; its format is reused, its numerical rates are not substituted for the database. Fixed-charge rules otherwise remain those of the original builders; applicability should be checked against the approved schedule.

ANG489 has only one supplied reading and requires another genuine dated reading before consumption can be calculated. It is not silently matched to the legacy AGN489 fallback spelling. Other missing or conflicting readings are listed by meter.

## Verification

Isolated PostgreSQL tests use the owner-reported rates/dates and explicitly seeded test band metadata. They verify consumption 31 kL, 1 kL, 15 kWh and 547 kWh, effective-date selection, ambiguity/missing rates, original calculation defaults, read-only repeated generation, unchanged token/property restrictions, and exclusion of all reconciliation figures. A local original-template PDF is generated through Playwright, parsed with pypdf and rendered with PyMuPDF for visual inspection. These are test records, not a live Almond financial statement.

No migration is required. No production records or tariffs were changed. After Render deploys, review /billing/utilities/26/generate-metsoa?billing_month=2026-10 and resolve only the specifically reported missing inputs through the appropriate authorised data workflow. No final live amount is certified by local tests.
