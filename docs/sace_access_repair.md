# SACE access repair ? local verification, 2026-09-23

## Identity and cause

Production identity supplied and verified by the owner: `sace_endorsement`, production subject 44, active/free/auto_enroll. Renielwe already has user 622 and active enrollment 1021. Enrollment did not establish the `AuthSubjectAdmin` grant read by the controller guard, causing the post-registration 403. Implementation resolves the subject by exact slug; tests deliberately use subject 900 and unrelated subject 44.

## Implemented journeys

- AIT's existing platform-admin-only `/admin/security/sace-management` page issues a signed, email-bound provisioning URL, valid for seven days. It does not create an account or grant rights just by issuing the URL. This URL is the authorization for an initial SACE administrator appointment; the public bare URL cannot appoint visitors.
- R follows that URL, accepts the existing administrator pledge and registers or signs into her existing account. Completion creates/reuses the exact `sace_endorsement` `AuthSubjectAdmin` email/subject grant and enrollment, and records the pledge. Existing stored pledges are reused. R reaches `/sace/provisioning` (Control Centre).
- Returning R logs in normally; the persistent grant is queried again and routes her to the Control Centre. No provisioning session or Auditor code is needed. No global approved-admin row or platform role is granted.
- Renielwe's existing registration can recover using a named link issued for her existing email. The application completes the missing grant; no manual production record modification is needed.
- Existing dashboard code generation and printable access slips remain. R cannot enter the Auditor journey accidentally.
- Auditor `/sace/join` checks invitation status and expiry, retains pending code/pledge through registration or login, and rechecks status and expiry under a row lock before claim. The existing invitation assignment links the Auditor to R's environment. Auditor access remains assignment-based and cannot generate administrator grants or codes.
- Endorsement registration uses free South African enrollment and bypasses commercial quoting.

No schema migration is required. Existing role, enrollment and interaction tables are used. No production writes or deployment were performed. No persistent disk videos were changed.

## Verification

The PostgreSQL HTTP-client tests use the actual SACE modules, actual authentication view functions, model columns and templates in an isolated Flask application. They use connection-local temporary tables cloned from local PostgreSQL, with separate temporary identity sequences. They do not call the full production app factory. Unrelated ORM relationships and the global page layout are omitted. These are route-level end-to-end tests, not a live browser/production smoke test.

| Check | Result |
|---|---|
| PostgreSQL provisioning, existing-account recovery, returning R login, code generation, Auditor registration/login/claim/board, invalid/expired invitations, scope boundaries | 12 passed |
| Existing SACE journey tests | 18 passed |
| SACE entry tests | 4 passed |
| Reading media regression | 6 passed |
| Report download regression | 5 passed |
| Startup enrollment preservation | 1 passed; 4 errors: isolated startup test execution lacks `os` in its namespace |
| Full suite: `python -B -m pytest tests -q` | Exit 2 during collection: UIP duplicate SQLAlchemy `user` table definition; suite did not execute |
| Diff whitespace check | Passed |

The full suite is not green. The unrelated startup and UIP collection issues remain parked; their source files were not changed as part of this repair. The repository-wide fixture also specifies SQLite despite the project's PostgreSQL requirement; it was not used as a fallback.

Commands: `python -B tests/support/sace_access_postgres_runner.py`, individual `tests/test_sace_journey.py`, `tests/test_sace_entry.py`, `tests/test_reading_media.py`, `tests/test_report_download.py`, `tests/test_startup_enrollment_preservation.py`; full suite as above. `tests/test_sace_access_postgres.py` provides subprocess isolation for collection.

## Exact repair files

1. `app/program_sace/access.py` ? new exact-subject authority, signed provisioning and authentication continuation helpers.
2. `app/program_sace/endorsement.py` ? controller helper and shared expiry/status validation.
3. `app/program_sace/endorsement_routes.py` ? separated routing/guards and claim validation.
4. `app/program_sace/routes.py` ? legitimate provisioning completion, existing-pledge recovery and Join validation/details.
5. `app/auth/routes.py` ? SACE registration/login continuation and return routing; this file already contained earlier pending pricing changes.
6. `app/services/users.py` ? preserve registration full name.
7. `app/admin/security/routes.py` ? existing-account-compatible named provisioning link issuance.
8. `templates/admin/security/sace_management.html` ? named administrator link form.
9. `templates/program_sace/provisioning_access.html` ? new informative access/sign-in page.
10. `templates/program_sace/auditor_pledge.html` ? authenticated Auditor name details.
11. `tests/test_sace_entry.py` ? entry regression expectations (previously untracked local file).
12. `tests/test_sace_access_postgres.py` ? isolated runner entry point.
13. `tests/support/sace_access_postgres_runner.py` ? PostgreSQL HTTP journey tests.
14. `app/quote/routes.py` ? existing three-line SACE quote bypass required by the entry regression tests.
15. `docs/sace_access_repair.md` ? this report.

Diagnostic outputs are under `artifacts/sace-*.txt`. Other pre-existing working-tree changes, including UIP, about/welcome and R2 work, are outside this repair. No unrelated files were reverted.

## Before deployment

Review these results before any deployment. A subsequent live smoke test should cover named-link recovery for the existing R account, normal returning login, generated/printed code, and Auditor Join/claim. This report does not claim live production validation.
