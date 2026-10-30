# Property Management Utilities redesign

## Staged delivery

1. c4080517 — per-property Utilities action, four-column property table, address wrapping; 8 focused PostgreSQL navigation tests.
2. 6a59ecf8 — Utilities label, selected-property workspace, four destinations and billing month; 6 focused PostgreSQL tests.
3. e2552cd5 — stable-ID configuration editor; owner contact updates, property-based account/meter creation; 10 focused PostgreSQL tests.
4. 4b27ad36 — supplying bulk relationships and individual-meter counts; 12 focused PostgreSQL tests.
5. 0d6420f0 — property-only Readings and append-only consumption capture; 15 focused PostgreSQL tests.
6. 3727d883 — actual/chargeable reconciliation, property-only meter collection; 18 focused PostgreSQL tests.
7. cc964a7b — existing METSOA/exception statements and corrected checkout model references; 20 focused PostgreSQL tests.
8. Final safety stage — remove legacy unit/tenant write paths; preserve historical financial particulars; 23 cumulative focused PostgreSQL tests plus dashboard regression checks.

Counts include reused regression tests; do not add them together. Tests run only on session-temporary PostgreSQL tables in ait_local_db. No production data, migrations, backfills or workers were executed. No live Render verification is claimed.

## Reuse and safety

Original setup template/routes, meter reading UI, consumption review, METSOA/exception templates and tariff builders are retained. Property ID is the only Utilities pivot. Setup saves accounts, owner details, meters and parent relationships using stable record IDs; omitting existing rows, renumbering historical identities, claiming NULL-property records or stale edits is rejected. New accounts and meters are not created from configuration examples. SQL account/meter history remains authoritative after setup save; old architecture drafts remain untouched.

Legacy tenant/unit AI onboarding and replacement-style final submissions are disabled. Existing readings/consumption are never deleted by setup or the reading API. Reading retries are idempotent; a conflicting existing billing period requires a separately reviewed correction process. Rates, arrears, arrangements, initial reading capture and historical exception records are view-only in Setup; use Readings for new reading capture. Existing statement tariffs and fixed-charge rules are not changed or automatically posted. Bulk reconciliation displays signed actual differences and max(0,difference), with Owner as the excess allocation; missing/duplicate/mismatched periods remain unconfirmed.

## Explicit limitations / production checks

- Historical accounts and meters with no verified property association remain unassigned and are not exposed through number/name guessing. Recovery requires separate verified evidence.
- Original wizard UI supports one bulk water and one bulk electricity meter with one bulk account. The parent-ID service supports multiple bulk records, but the UI does not safely edit configurations beyond its existing capacity; retain those read-only.
- Existing account numbers, meter numbers, meter types and account assignments are protected; correction/reassignment of historical identities is not implemented.
- If legacy bil_meter.sectional_unit_id remains NOT NULL, property-only new meter creation stops with HTTP 503 before any write. Check its nullability read-only before live setup. No additional migration was introduced without that evidence; a reviewed permissive schema change would be needed if mandatory.
- Production metsoa_setup_001 is not rerun or replaced.
- Existing statement checkout still requires the established manager-level unlock; token-cost rules remain unchanged.
- Email/PDF rendering infrastructure is retained. Tests exercise actual statement routes/templates with isolated statement-builder boundaries; live email delivery and Render deployment are not verified.
