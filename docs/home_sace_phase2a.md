# HOME Phase 2A operational lifecycle

Phase 2A is local implementation only. Reading implementation is frozen. HOME
educational content, examination wrappers and new manuals remain Phase 2B work.

## Resume inventory

The interrupted working tree already contained changes to shared login and public
bridge role filtering; HOME models, auth, routes and service; the Control Centre;
and the foundation test harness. It also contained the new lifecycle service,
`home_sace_002` migration, completion confirmation, explicit finalizer and 17
Phase 2A tests. These were retained. The initial 17 lifecycle tests passed.
Unrelated patch scripts and artifact repositories were not edited.

Resume work tightened Auditor authority to require the linked active appointment,
active controller identity and exact HOME grant; prevented a retained Reading code
from bypassing the dual-authority check; added two regressions and a discoverable
lifecycle test entry point; and repaired legacy grant-only Reading fixtures in
HOME-owned tests using the existing Reading provisioning helper.

## Migration and subject

`home_sace_002` follows `home_sace_001` on the HOME branch. It creates engagement,
controller appointment and audit-event tables, plus a nullable appointment FK on
HOME invitations. Historical identities/invitations are not adopted as authority.
No migration stamp, production migration or application startup was run.

The migration inserts `sace_home_endorsement` by unique slug, with database-generated
ID, active/free/manual configuration, hidden bridge/welcome presentation and HOME
entry endpoints. `ON CONFLICT (slug) DO NOTHING` preserves all existing settings,
including an inactive subject. Runtime authority resolves the active subject by
slug; ordinary `home` and Reading `sace_endorsement` are never substitutes.
Downgrade refuses when lifecycle history exists and retains the subject even when
the new tables are empty. It never removes a possibly pre-existing subject.

## Authority and login

R enters through `/sace/home/provisioning` using a HOME-only, 15-minute session
nonce. The HOME pledge and authentication precede its single-use database claim.
Existing email-bound, expiring provisioning invitations remain supported as optional
entry links; the standard URL requires no operator issuance. Pledge, exact operational grant, controller
appointment, engagement and audit event are created atomically. A HOME grant or
identity alone, platform administrator role, enrollment or session flag is not
authority. Returning HOME-only R login routes to `/sace/home/control`.

Auditor invitations link to the issuing appointment. Pledge and one-time claim
create the HOME assignment and evidence without an Auditor admin grant. Access
requires an active assignment, active parent appointment/controller, the exact
HOME grant and operational parent engagement. Returning HOME-only Auditor login
routes to `/sace/home/`, then the board when exactly one assignment is available.
Completed Auditor examinations cease operational access and do not complete R's
engagement or retire R's grant. Authentication and successful protected access
are recorded in HOME audit events.

Safe explicit HOME destinations and current HOME pledge/auth continuations retain
their HOME destinations. Explicit Reading destinations use the existing Reading
flow. A bare login with active authority in both activities returns HTTP 409 with
Reading and HOME destination links stated in the response; a retained Reading code
cannot silently select Reading. Expired/revoked authority does not count as active.

## Completion

R's Complete Activity Endorsement screen asks for Yes/No. No changes nothing.
Yes records requester, request time and exactly 48 hours until the deadline, with
an audit event. Both roles retain access strictly before the deadline. R may cancel
before it, restoring active state while preserving request/cancellation audit
history; another request starts a new 48-hour period. Repeated Yes is a conflict.

At or after the deadline both roles lose operational access even when housekeeping
has not run. Cancellation and invitation claims also fail. Explicit housekeeping
finalizes due engagements, ends appointments and deletes only their exact HOME
operational grants. Identity, pledges, invitations, assignments, evidence and
grant-at-issue history remain. Grant mismatch aborts for review. Repeated finalizing
does nothing; errors roll back atomically. Subject locking serializes HOME changes
and concurrent finalizers. A fresh approved provisioning invitation can establish
a new engagement after finalization without reviving old assignments.

`scripts/finalize_home_completions.py` requires DATABASE_URL, expected database,
expected role and `--execute`; it bypasses the application factory. It was not run.
Scheduling/deployment remains a later operational step. Access denial at the
deadline does not depend on that scheduling.

## Isolation

HOME uses its own tables, tokens, pledge state, codes, documents, evidence and
completion service. Reading source files and migrations are unchanged. HOME
operational grants are excluded from shared subject-admin session classification.
Finalization does not alter Reading or ordinary HOME grants or educational progress.

## Verification on 2026-10-03

- `python -B tests/support/home_sace_lifecycle_runner.py`: 19 passed.
- `python -B tests/support/home_sace_postgres_runner.py`: 14 passed.
- `python -B tests/support/home_sace_continuation_runner.py`: 10 passed.
- `python -B tests/support/sace_reading_home_isolation_runner.py`: 24 passed.
- HOME diff whitespace check passed.

Fixtures guard localhost `ait_local_db`; application factory/startup is bypassed.
Most writes use connection-local temporary tables. Concurrency tests use uniquely
named local fixture tables and remove them. Migration tests use a temporary
namespace and rollback. No production connection/data change was made.

The initial foundation run had 13 passes and one failure from a grant-only Reading
fixture expecting lifecycle authority. The corrected fixture passes. A Reading
`datetime.utcnow()` deprecation warning remains; Reading is frozen. The full
repository suite and production migration/deployment were not run.

## Recommended commit manifest

Include only the following Phase 2A paths after review; do not stage the whole tree:

- `app/auth/routes.py`
- `app/models/sace_home.py`
- `app/program_sace_home/auth.py`
- `app/program_sace_home/lifecycle.py`
- `app/program_sace_home/routes.py`
- `app/program_sace_home/service.py`
- `app/public/routes.py`
- `migrations/versions/home_sace_002_lifecycle.py`
- `scripts/finalize_home_completions.py`
- `templates/program_sace_home/control.html`
- `templates/program_sace_home/completion_confirm.html`
- `tests/support/home_sace_postgres_runner.py`
- `tests/support/home_sace_continuation_runner.py`
- `tests/support/home_sace_lifecycle_runner.py`
- `tests/test_home_sace_lifecycle.py`
- `docs/home_sace_foundation.md`
- `docs/home_sace_phase2a.md`

Suggested commit: `Implement isolated HOME Phase 2A authority and completion lifecycle`.
No commit, push or deployment was performed.
