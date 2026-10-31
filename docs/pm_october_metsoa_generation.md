# Original tariff METSOA reconnection

The existing PM Generate METSOA URL remains /billing/utilities/<property_id>/generate-metsoa?billing_month=YYYY-MM. Add &format=pdf for an inline PDF generated from the same original metsoa.html template. The Utilities navigation has an adjacent YYYY-MM billing-month input. It defaults to the current South African month through the existing page context, supports historical periods and submits the property-specific Generate METSOA route using GET. Property access and the PM token-free policy are unchanged. Legacy statement/token routes are unchanged.

## Sources

The PM adapter reads registered property meters and municipal account pointers, canonical bil_meter_reading pairs, bil_tariff and verified registered tenant associations for the statement recipient. It does not read Metro Reconciliation statement figures or payments, and never reads/writes historical bil_consumption for generation. It never invokes generate_consumption_records_from_readings or any posting function.

Existing build_electrical_rows, build_water_rows and water/sanitation detail builders accept optional scoped in-memory records and explicit tariffs. Their legacy callers keep their original defaults. PM uses Decimal values and effective database records; the confirmed recurring surcharge rule is used; no unverified water-loss, management or refuse fallback is used. The original electricity summary, water summary, side-by-side water/sanitation band details and final-total position are retained. PM rates are printed without the original one-decimal water-rate rounding. Missing inputs produce a prominent INCOMPLETE marker and withhold the final amount due; any displayed figures are confirmed components only. Summary and tariff details print on separate pages.

## Effective dates and discrepancies

For the selected billing month, the statement date is its last day. Choose the latest effective record per utility/code on or before that date; future versions are excluded. Duplicate latest versions, multiple electricity tariffs, invalid rates/dates/band metadata, insufficient band coverage, or tariff changes crossing a reading period require review rather than guessed allocation. Actual reading dates are retained.

The owner reported 13 live tariffs: ELEC01 R3.2795 effective 2026-01-01; W01-W05 R39.39/46.70/62.17/95.91/105.39; S01-S04 R5.45/9.20/17.54/27.38; W_LOSS R19.30, W_MGT R144.54 and S_REFUSE R206.93 effective 2026-06-05. These effective dates precede October 2026. Full live band/unit/daily-allocation metadata was not supplied in that report and remains subject to runtime validation and owner confirmation against the approved tariff schedule.

The owner has now confirmed that R1.48/kL is the established recurring monthly water surcharge and, separately, R1.48/kL is the recurring sewerage surcharge. The existing builders apply both independently to each water meter's validated consumption. No new database tariff or migration is needed to activate this confirmed business rule. Optional database surcharge records that contradict it are flagged for review rather than silently changing the rule.

W_LOSS is the confirmed fixed per-water-meter insurance levy. Its latest unambiguous effective database rate remains authoritative (R19.30 for the supplied October records, rather than the historical PDF's R17.12). The previous selector could suppress W_LOSS when legacy utility_type/unit labels were absent or outside a narrow whitelist. The selector now recognises the established W_LOSS code as fixed insurance without rewriting that metadata or multiplying the levy by kL. Versions across legacy utility labels are resolved together, and duplicate latest versions are blocked to prevent double charging. Other fixed-charge rules remain unchanged.

The original statement layout, tiered calculations and subtotals remain intact. The historical PDF's other rates are not substituted for applicable database rates.

The current owner reports ANG489 consumption of 184 kL. Generation still requires the actual canonical dated reading pair; it does not copy a displayed consumption value into history or invent a missing reading. The isolated 184 kL test verifies R272.32 water surcharge, R272.32 sewerage surcharge and R19.30 insurance. It is not silently matched to the legacy AGN489 fallback spelling. Other missing or conflicting readings are listed by meter.

## Verification

Isolated PostgreSQL tests use the owner-reported rates/dates and explicitly seeded test band metadata. They verify consumption 31 kL, 1 kL, 15 kWh and 547 kWh, effective-date selection, ambiguity/missing rates, original calculation defaults, read-only repeated generation, unchanged token/property restrictions, and exclusion of all reconciliation figures. A local original-template PDF is generated through Playwright, parsed with pypdf and rendered with PyMuPDF for visual inspection. These are test records, not a live Almond financial statement.

No migration is required. No production records or tariffs were changed. After Render deploys, review /billing/utilities/26/generate-metsoa?billing_month=2026-10 and resolve only the specifically reported missing inputs through the appropriate authorised data workflow. No final live amount is certified by local tests.

## METSOA recipient

Municipal account ownership is not the statement recipient. The PM renderer reuses the existing read-only property/manager-scoped tenant retrieval, including direct register links, legacy lease links and saved Lease Kit tenant IDs. It displays the registered tenant name and email, never a municipal owner name or a draft name substituted by name matching. No account owner, tenant, lease or ledger record is changed.

A single current verified candidate can be selected automatically. Multiple candidates require explicit tenant selection. Historical billing months require explicit selection because a current property link alone does not establish historical occupancy. A superseded draft-only association is not automatically assumed current. The selected registered tenant ID is validated against the authorised property's candidates and retained in the generated PDF URL. Missing tenant associations or unverifiable register ownership are reported on the statement. A missing email is displayed as Not provided.

No recipient settings table or migration is needed: this is a validated statement-view selection. Live recipient associations were not inspected during implementation. If Almond has no reliable registered tenant link, confirm the existing register/property or saved lease association through its existing workflow; no association is manufactured by generation.
