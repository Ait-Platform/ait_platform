# Recovered Almond tariff schedules

The source assignments are preserved as decimal strings in `app/program_billing/recovered_almond_tariffs.json`. They are inactive review data, not imported by the live tariff selector. No database rows, migrations or production changes accompany this feature.

The existing Python calculator is authoritative. A prepared rate version copies its existing tariff family's units, boundaries and factors through the normal Admin save service. Only the rate, entered effective date, description and source evidence are prepared; there is no new calculation engine or tariff category.

Schedule B has the supplied start 2024-11-01. Its end is determined by the next stored version of each existing component, and must be reviewed against the actual source. Schedule A has a stated cutoff of 2024-10-31 but no supplied start. Its rate cannot be prepared until the corresponding B boundary version exists; Admin must then enter the actual A start. No date is inferred or automatically backdated. A B version is not prepared unless a later stored version bounds it, so the latest captured schedule cannot be displaced.

## Mappings

- `EleRate`: the unique existing electricity family; A 2.58, B 3.5747.
- `WsRate200/833/1000/1500`: existing water bands with those stored upper boundaries. Four rate versions only; no fifth water rate is provided or invented.
- `SdRate200/833/1000/1500`: existing sewerage/sanitation bands with those stored upper boundaries. Existing reduction/allocation metadata is preserved, including an already-unbounded final S04 band. No new boundary is manufactured.
- `WtrSurcharge`: existing dedicated Water Infrastructure Surcharge family, or the established WSSurcharge alias. Unit must already be verified R/kL. Existing full-water-volume Python calculation remains authoritative.
- `WtrLossLevy`: existing W_LOSS / WaterLossLevy identity or the explicit administrator Water Loss category. Existing flat-charge basis remains unchanged.
- `SDSurcharge`: existing dedicated sewerage surcharge or SDSurcharge alias with verified R/kL units. Uses the active AIT calculation; conflicting dormant Python helpers are not re-enabled. No sewerage applicability or category is created if none exists.
- `SdBinCharge`: existing S_REFUSE / RefuseBin or explicitly categorised refuse component; existing flat-charge application remains unchanged.

## Withheld values

All Schedule A water/sewerage `Rate*` and `Unit*` settings lack unambiguous equivalents in the current AIT tier model. In particular Unit25=19 cannot be assumed to define a 25 kL tier. Original values remain visible and are not activated.

Schedule B `Unit*` settings are not substituted for existing boundaries or daily allocation factors. `Tier1Pc/Tier2Pc/Tier3Pc` have no verified equivalent in the active factor semantics and are not applied. `MthMgtFee14=24.09` is not substituted for a generic management fee: the suffix 14 and its quantity/billing basis are unresolved.

Mappings are component mappings, not certification that a recovered value is an official municipal tariff. Existing current versions and evidence remain intact. An explicitly reviewed Save appends a dated version with recovered-variable provenance in the existing verification table. GET review/preparation cannot write. Existing financial statements are not regenerated or posted.
