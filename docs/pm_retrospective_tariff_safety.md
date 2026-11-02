# Automatic tariff codes and retrospective correction safety gate

## Completed: automatic codes

The Add form displays read-only previews for each selected type. Save ignores any browser-supplied code and allocates the final code under a PostgreSQL transaction advisory lock for that type, followed by the existing identity lock. Numeric suffix allocation considers all stored versions but does not count versions as new identities. Existing named codes remain unchanged. Prefixes are ELEC, W, S, WF (water fixed), SF (sewerage fixed), with at least two digits. Editing retains the original identity. Concurrent independent-session tests exercise final allocation, not only previews.

No migration is needed for this stage. Existing rate version rules, CSRF, administrator checks, factors, effective dates and billing formulas are unchanged.

## Explicit stop: retrospective correction prerequisites

Stages 2-5 have NOT been enabled. The request requires a stop if the current schema cannot safely distinguish superseded corrections or immutable issued billing evidence cannot be established.

Verified repository findings:

* `app/models/billing.py::BilTariff` has a start date but no recorded timestamp, actor or supersession link.
* `app/utils/tariff_versions.py::resolve` chooses the latest applicable start; two rows with the same identity/start are deliberately ambiguous. It cannot represent an audited correction replacing an already stored effective-date version without additional schema/selection semantics.
* `app/admin/programs/billing/tariff_management.py::save_rate` remains append-only and rejects backdating. Do not merely remove this check.
* `app/models/billing.py::BilStatement` declares only ID and tenant link, not an immutable issued line snapshot.
* `app/program_billing/lease_soa_statement.py::statement` projects the current ledger. It does not establish which electricity quantity, tariff version, VAT or rounding produced a previously issued document.
* `app/utils/billing_persist.py::_upsert_metsoa_row` updates existing monthly results; `_replace_breakdown` deletes and replaces breakdown rows. These mutable caches must not be treated as immutable issuance evidence, and were not invoked or modified.
* `app/program_billing/utilities_output.py` provides a short-lived signed preview digest, not a durable issued-SOA snapshot or original tenant electricity billing evidence.

Read-only catalog inspection of the verified local ait_local_db also confirms the tariff columns have no recorded-at/supersession metadata and bil_statement contains only id and tenant_id. Production schema and issued artifacts were not inspected. No production access or financial writes were used.

## Required design before further implementation

1. An additive tariff correction audit structure: original version ID, new version ID, legal effective-from, recorded-at (UTC), actor, reason and explicit supersession lineage. Define deterministic applicability for a correction sharing an existing effective start while retaining as-recorded history. Unknown historical recording times must remain unknown; do not backfill invented dates.
2. Immutable issued-SOA identity and line snapshots: property, tenant, billing period, original billed kWh, tariff version/rate, VAT basis/rate, rounding rule, component amounts, issue timestamp and immutable document hash/private storage key. First determine whether existing production-issued artifacts provide this evidence; otherwise affected historical rows must remain Cannot Calculate - Review Required.
3. Reproducible impact records linked to that exact issued snapshot and correction, including the calculation inputs/rules/results and review state. A later posted adjustment must use the existing PM ledger, not a new ledger.
4. An idempotent owner decision record with actor/time, property/tenant/period, signed difference and decision. Absorption must not debit a tenant; proposed recovery/credit must remain unposted until separately approved. Decreases need credit/refund treatment, never a subsidy label.
5. Database constraints and transactions preventing duplicate corrections/decisions and ambiguous applicability. Migration and resolver design must be reviewed before enabling backdates. Do not change issued documents or regenerate them to fabricate evidence.

No migration is prepared or applied at this stop gate. Production schema/evidence verification and approval of the above design are required before continuing. Setup Wizard and all excluded functions remain unchanged.
