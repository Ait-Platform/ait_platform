# Annual tariff register — individual activation

Stage 2 adds an explicit administrator confirmation for one component at a time.
It does not activate production records during installation, GET requests, builds
or tests. No new database model, migration, calculator or batch activation exists.

## Verified scope

Only 2026/27 S01–S05 and W01–W05, effective 1 July 2026, are eligible.
The existing candidate CSV retains every original rate and gains source evidence
for these ten rows. All historical HOLD items and other charges remain excluded,
including electricity, W_MGT, refuse, water loss and surcharges.

Official source: [eThekwini final 2026/27 tariff tables](https://www.durban.gov.za/uploads/0000/13/2026/06/22/eth-final-2026-2027-tariff-tables.pdf).
Downloaded and visually checked on 10 November 2026:

- Water: printed page 265 / PDF page 268; R44.10, R52.30, R69.60, R107.40,
  R118.00 per kL excluding VAT.
- Sewerage: printed page 281 / PDF page 284; R6.53, R11.00, R21.00, R32.80,
  R37.60 per kL of discharged effluent excluding VAT.
- Domestic discharge: printed page 284 / PDF page 287; 95%, 75%, 75%, 65%, 60%.
- Source SHA256:
  `59b640ec20d9a8a408ebd92a378dae66632470e703a7f3fed2509a5bf9c6a56e`.

The category is direct domestic supply without individual break-pressure tanks;
the first-band valuation exemption must not apply. The sewerage fractions require
the qualifying domestic development not exceeding two storeys and no different
discharge agreement. Official bands are monthly 6/25/30/45 kL, calculated daily.
AIT's existing daily allocation conventions remain unchanged. The confirmation
requires the administrator to verify applicability, not merely that a rate matches.

## Behaviour and safety

`/admin/billing/tariffs/annual/<code>/activate` GET displays a confirmation only.
POST requires existing administrator authority, CSRF, an explicit checkbox and a
15-minute signed token bound to the administrator, component, candidate evidence,
review date, all stored family versions and their verification records.

The confirmation displays today's active version, the proposed rate, exact start,
the earlier version superseded, source/category/band/unit evidence, preserved
dimensions, any later version boundary, and the effect on regenerated reports.
Cancel and GET never change tariff records.

Saving locks the same family as ordinary tariff administration, re-reads its rows
and evidence and rejects stale snapshots or duplicate effective starts. Existing
`insert_version` inserts the new tariff and existing verification evidence in one
transaction. Audit failure rolls back both. Prior versions/evidence are never
updated or deleted. The audit records actor/timestamps through the established
service and includes official source, checksum, band, discharge basis, superseded
version and approval snapshot.

Existing family dimensions are copied unchanged. Unknown units, invalid metadata,
conflicting identities/categories or absent source evidence block activation.
Legacy S04 factor 999 remains unchanged: the corrected sewerage calculator applies
its own finite fourth-band width and separate fifth band. A genuinely missing band
can be inserted with its existing canonical code, using the verified band and
established AIT metadata conventions; no replacement meter or duplicate category
is created. Existing later tariff periods are preserved.

## Deployment boundary and outstanding checks

No production database was accessed or written during implementation. Tests use
temporary records plus disposable local PostgreSQL concurrency tables. Browser,
date/version, stale/replay, rollback and unchanged calculator/screen/PDF regressions
cover this mechanism.

Before any separately authorised production activation, inspect actual stored
units, bands, identities and source/category evidence. A pre-existing 1 July 2026
version is a conflict and cannot be overwritten here. Missing verification storage
or unknown units remains a blocker rather than triggering a migration or silent
metadata correction. Historical water-source conflicts and management/refuse
questions remain HOLD. W_MGT's amount and meter allocation are untouched.
