# ProTrade Stage 1: saved shops in the Client Register

Date: 2026-11-12

The register queried only `mech_clients`, while Shop Profile Setup writes
`mech_shops` and synchronizes a Company Register sender profile. Saving a shop
therefore could not produce a Client Register row. The older Happiness regression
checked the Company Register and did not assert Client Register membership.

The dashboard now includes named shop records belonging to the authenticated
owner, directly from `mech_shops`, alongside existing customers. Draft-status
legacy profiles are included. Each row links to its exact saved shop profile;
profile edits post its ID and validate ownership. Per-trade selection continues
to reuse existing records. No shop/customer copies, backfill, schema migration,
or production database writes are required by this change. Existing duplicate
records, if any, are retained rather than merged or deleted without review.

Verification: `.venv/Scripts/python.exe tests/test_protrade_shop_register.py`
passes five regressions using actual route bodies and the dashboard template
against temporary tables in verified localhost `ait_local_db`. The transaction
is rolled back. Checks cover legacy saved-shop visibility, existing customer
visibility, owner isolation, repeat saves, rename identity, exact same-trade
record selection, multiple trades, and register pagination/counters.
`git diff --check` passes.

A read-only local query found no shop name containing `happ`. The actual live
Happy Shop row and its owner have not been independently verified. Production
verification remains pending deployment and a check using its owner's account.
If database inspection is needed, use the current Render resource configuration
as required by agent.md, never credentials from maintenance scripts. Confirm the
existing Happy Shop ID/owner are unchanged and that one row for that record
appears in its owner's Client Register after reload.

Carpentry and BOQ work remain on hold until Stage 1 live verification.
