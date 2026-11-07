# METSOA allocation and statement review

Reviewed supplied files in tmp/Metsoa (read-only): `2026-10-31  MetSoa .pdf` (three pages) and `MetSoa Statement.pdf` (six pages).

## The statements cover different periods

The original covers October 2026; the AIT PDF covers February 2026 and explicitly uses current tariffs as a historical estimate. Their consumption and rates differ. October's printed grand total is not an appropriate benchmark for February.

## Original visible-line arithmetic

Amounts below independently sum the displayed charge lines, rounded to cents. They are a document comparison, not a replacement calculation against production records.

| Meter | Water | Sewerage | Combined |
| --- | ---: | ---: | ---: |
| BLZ080 | 3897.64 | 1003.47 | 4901.11 |
| AGN489 | 695.22 | 310.45 | 1005.67 |
| CXN998 | 100.28 | 13.86 | 114.14 |
| CWR826 | 560.48 | 105.66 | 666.14 |
| CWR820 | 1099.26 | 223.14 | 1322.40 |
| CWA388 | 462.52 | 84.30 | 546.82 |
| CWA074 | 6480.64 | 1724.97 | 8205.61 |

Electricity: 15, 113, 300, 389, 530 and 625 kWh at 3.5747, each rounded to cents: 53.62 + 403.94 + 1072.41 + 1390.56 + 1894.59 + 2234.19 = **7049.31**.

Visible-line total: **23811.20**, versus printed **23811.19**. Some printed water subtotals differ by cents. The original also shows a bulk reading difference of 118 kL (15161 - 15043) but prices 11.8 kL. The other water readings total 178 kL. The document does not establish why 11.8 was used. Some sewerage rate labels repeat water rates even though the amounts use different rates. Water Loss Levy displays consumption/surcharge units alongside a fixed 17.12 amount. These inconsistencies are not copied into AIT's calculations or rate labels. Existing Python tiers, signed bulk differences and pricing safeguards remain unchanged.

Management fee: 6 x 24.09 = **144.54**, once. Refuse: **206.93**, once. Existing AIT fixed monthly tariff records already store these flat monthly amounts; the fix does not multiply them again or invent a unit count or unit tariff.

## Supplied AIT allocation correction

Seven water meters each received management 144.54 and refuse 206.93. Six excess copies total **2108.82**. Keeping all other supplied figures unchanged gives **13823.24 - 2108.82 = 11714.42**, still an **ESTIMATED** total. An isolated 13-meter regression reproduces the February consumptions, dated fixture rates, absent sewerage surcharge, negative bulk difference (-3 kL), electricity subtotal 5935.89 and corrected total 11714.42. No production data is read or changed by that test.

## Implementation and safeguards

The shared override fee builder previously copied every water/sewerage fixed tariff onto every water meter. Property management and refuse now apply only to one identified bulk water meter, using existing confirmed links or an explicitly marked bulk account's saved water-meter pointer. No name, meter-number spelling or record order is used to guess the target. Missing or conflicting targets retain incomplete status and explanatory warnings. No links or audit confirmations are created.

Water Loss Levy remains per meter. Infrastructure surcharges retain independent dated per-kL rates; absent sewerage surcharge retains the applicability warning. Property-only tariff changes do not invalidate unrelated individual-meter intervals. Historical overrides, tariff effective dates, audit data and access checks remain intact.

Modern PM reports use one summary with all saved reading pairs, then compact side-by-side water/sewerage calculations, and the existing municipal billing comparison. Natural pagination replaces forced breaks and duplicate reading tables. The legacy report branch remains unchanged. Status, unpriced components and historical estimate labels remain visible on screen and in PDF.
