# LOSS email security ? Stage 1

Only `/loss/report/send` and `/loss/report.email/<run_id>` are changed. Both require Flask-Login authentication and resolve the report owner from `lca_run`, ignoring submitted recipient and user identifiers. Foreign and missing reports return the same controlled error before PDF rendering.

Cross-owner access requires a current persisted administrator grant: an active `auth_approved_admin` entry, or an `auth_subject_admin` entry for the active LOSS subject. Session roles, referrers and unrelated subject grants do not authorize sending. The grant is rechecked immediately before reserving the send. Inconsistent report/result/response ownership is refused.

Recipients are taken only from the authorised owner's saved, syntactically valid account email. As explicitly agreed for Stage 1, this is not proof of mailbox verification or ownership. No email-verification system was added.

## Duplicate protection

Both routes share a PostgreSQL owner-level advisory transaction lock and a durable `loss_report_email_send` reservation. The database clock enforces a 600-second cooldown across both routes and all reports for that owner. This blocks repeated/concurrent sends across workers and restarts. Successful deliveries may be deliberately submitted again after the cooldown; this is not permanent deduplication.

The reservation is committed before SMTP. Missing tracking storage or reservation failure prevents sending. SMTP failure, uncertain delivery, process interruption, or failure to record transport acceptance leaves `attempted`/`uncertain` state, which blocks another send of that report even after cooldown. Administrative recovery requires checking delivery evidence first; there is no automatic retry. Transport acceptance does not prove inbox delivery.

The existing report builders, PDF renderers, filename formats and standalone PDF routes remain unchanged. Authorised successful sends still return their PDF download. The legacy `EMAIL_ENABLED=False` path remains download-only, with authentication and ownership checks. Sending/PDF/storage failures return controlled errors without SMTP or database details.

## Required migration

`loss_email_security_001` creates only the tracking table and its owner/timestamp index. It is an independent LOSS Alembic branch and does not require a ProTrade migration. Existing report data is not updated. Destructive downgrade is intentionally disabled to retain send protection state.

Apply using the Render service's assigned PostgreSQL connection, after independently verifying `current_database()` and `current_user`:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'loss_email_security_001')"
```

Verify `SELECT to_regclass('loss_report_email_send');` and the new Alembic revision. Apply this targeted revision rather than upgrading unrelated branches. Without it the two email routes fail closed. The migration was applied successfully to verified local `ait_local_db`; production execution has not been verified.

## Validation

24 focused checks passed: 22 PostgreSQL tests plus the existing removed-debug-mail-route and Flask-Mail configuration regressions. PostgreSQL tests execute the actual migration, guard and route functions against isolated generated table names. They cover anonymous access, foreign and missing reports, forged session permissions, active/inactive global and LOSS admin grants, recipient/user-ID manipulation, owner-wide cooldown, concurrent workers, advisory-lock contention, account changes and admin revocation during PDF rendering, invalid/PDF failures, SMTP failure, suppressed delivery, reservation/outcome commit failures, missing tracking storage, CSRF, authorised PDF downloads and source-record preservation. PDF generation and SMTP are mocked in the route tests; no live mail was sent. Standalone report/PDF functions were checked to remain unchanged.

A local migration before/after comparison also confirmed that every existing `lca_run`, `lca_result` and `lca_response` row was preserved. Credentials and mail configuration were not changed. Contact, password reset, other LOSS actions and other programs are outside this stage.
