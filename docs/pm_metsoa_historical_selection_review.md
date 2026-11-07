# February 2026 historical tariff selection: local verification

## Scope and root cause

Read-only inspection of `ait_local_db` and source code on 7 November 2026. Production access was unavailable and the user explicitly chose local verification only. No production access was retried. Property 26 does not exist locally, so an actual saved Property 26 statement cannot be regenerated from this database.

Schedule A and Schedule B are **inactive JSON review data**, not inactive rows in `bil_tariff`. The table has no activation flag. Only dated rows saved through tariff management participate in METSOA selection. No November 2024 tariff rows exist locally. Schedule A also lacks a verified start date.

`metsoa_tariffs.select` and `tariff_versions.resolve` already select the latest effective start on or before the reporting month's end, per component. An explicit current-rate override only fills missing historical families; it does not replace an existing historical version. No confirmed selector defect warrants changing that logic.

The local electricity record is **ELEC01, 3.2795, effective 2026-01-01**, while the water, sewerage and fixed-charge records start **2026-06-05**. A saved November 2024 electricity version would be superseded by that January record for February. This is a stored-date discrepancy with the stated three-schedule policy, not a lookup defect. No effective dates were changed.

## Verified recovered Schedule B mapping

| Supplied variable | Existing local component | Historical rate | Local readiness |
| --- | --- | ---: | --- |
| EleRate | ELEC01 | 3.5747 | Can prepare; January 2026 competing version must be understood |
| WsRate200 | W01 | 40.1 | Can prepare |
| WsRate833 | W02 | 47.5 | Can prepare |
| WsRate1000 | W03 | 63.3 | Can prepare |
| WsRate1500 | W04 | 101.84 | Can prepare |
| SdRate200 | S01 | 5.45 | Can prepare |
| SdRate833 | S02 | 9.2 | Can prepare |
| SdRate1000 | S03 | 17.54 | Can prepare |
| SdRate1500 | S04 | 27.38 | Can prepare |
| WtrLossLevy | W_LOSS | 17.12 per meter | Can prepare |
| SdBinCharge | S_REFUSE | 206.93 once on bulk | Can prepare |
| WtrSurcharge | No local surcharge tariff row | 1.48 per kL supplied | Existing Admin type supported, but no stored target version locally |
| SDSurcharge | No local surcharge tariff row | 1.48 per kL supplied | No stored target version locally; applicability must not be assumed |
| MthMgtFee14 | Unresolved | 24.09 supplied | Not a verified flat monthly W_MGT mapping; no activation |
| Fifth water tier | W05 | Not supplied | No historical rate invented |

The Unit and Tier percentage assignments are retained as source material, not substituted into Python tier factors. Management's original six-unit calculation does not establish a dated historical flat-rate record for February by itself.

## February calculations and tests

All integration calculations use isolated temporary PostgreSQL tables. Meter consumptions reproduce the supplied February PDF: six electricity meters totalling 1810 kWh; water bulk 82 kL minus linked individuals 85 kL gives **-3 kL**, informational, with the existing nonnegative pricing floor unchanged. These fixture readings are never imported into production.

* With only the eleven locally mappable B versions saved in the fixture (and all current fixture versions dated June as stated in the requested policy), electricity is **6470.21** and available charges total **12067.37**. Water infrastructure surcharge and management are unavailable; sewerage surcharge is unconfigured. This is **INCOMPLETE**, not a final amount due.
* With independently stored historical water and sewerage surcharge fixture rows at 1.48/kL as well, available charges total **12318.97**: electricity **6470.21** + water/sewerage **5848.76**. W_MGT and W05 have no historical versions. This is also **INCOMPLETE**, with no ESTIMATED banner because no current tariff is applied. W05's rate is not consumed by this month's individual readings, but remains a catalogue coverage warning; W_MGT is an actual missing charge.
* Explicit fallback after those B versions estimates only W_MGT and W05; all thirteen applicable stored B versions remain selected. The resulting **12463.51** is **ESTIMATED**, not a verified historical total.
* A separate synthetic full-history fixture supplies independently saved historical management/fifth-tier records solely to test FINAL status: no ESTIMATED banner appears even if the fallback checkbox is selected. Those fixture records are **not** claimed to be verified recovered mappings.
* A regression reproduces the actual local January electricity date and confirms that it supersedes November according to the saved dates.

The original **11714.42** June-rate estimate is not reused as a historical benchmark. Management/refuse allocation, water-loss per-meter allocation, dated tier factors/units, negative bulk warnings, user isolation and compact PDF layout are covered. Two pages of the full-history fixture PDF were visually reviewed; natural pagination is unchanged.

## Delivered change and remaining data work

The active compact template's repetitive explanatory paragraph is shortened to one current-schedule sentence, only when an actual override exists. Regression coverage verifies B precedence, no automatic catalogue activation, exact unavailable components, complete/incomplete/estimated labels, screen/PDF consistency and cross-user denial. The selector and calculator are unchanged.

No tariff rows, dates, readings, meter links, audit evidence, financial transactions or migrations were modified outside temporary fixtures. This commit does not activate Schedule B in any environment. A production fix still requires verifying the real tariff inventory and saving supported historical versions through existing Admin infrastructure; unsupported mappings and competing effective dates must not be silently resolved. No verified final total for the actual Property 26 February statement is claimed.
