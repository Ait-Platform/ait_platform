# RCM Waiting Room and home association (local implementation)

Scope: Other enrolment, consent, limited owner discovery, home-specific decisions,
explicit reconsideration, and durable home approval. This does not implement
relationships, staff-role assignment, working contexts or commercial entitlement.

## Schema and transition

Run `scripts/retirement_waiting_room_transition.py apply` with the local virtual
environment. It accepts only the local `.env` connection to loopback PostgreSQL,
`ait_local_db`, port 5432, public schema; inherited production URLs are not used.
Database identity and the deployed legacy schema are verified before writes.
Unknown dependencies, schema drift or partial installation fail closed.

The transaction adds:

- `retirement_waiting_user`: unique user FK, preferred name, optional home clue,
  consent and consent/create/update timestamps. No organisation FK.
- `retirement_association_review`: home and waiting-user FKs, decision, reviewer
  FK, timestamp and positive version. Unique `(organisation_id, waiting_user_id,
  version)`. A PostgreSQL trigger rejects UPDATE and DELETE of audit rows.
- Membership approval timestamp and approving-user FK; legacy status becomes
  nullable. Paired approval fields are required together. A NULL-status row must
  have positive approval and no legacy requested/approved roles.

All existing membership status/role checks and person-home uniqueness remain.
No invented association lifecycle status exists. Repeat application validates
the complete schema and performs no changes. `verify` is read-only. No Alembic
chain is executed or stamped, and no application factory or create_all is used.

Existing rows are preserved without speculative approval backfill. Legacy rows
without trustworthy evidence remain for later explicit reconciliation. New
founding declarations record contemporary association approval atomically with
the existing owner membership; existing owner status/roles are preserved.

## Behaviour and security

`/retire/other` requires login and leads to `/retire/waiting-room`. Initial
enrolment requires explicit unchecked consent. Updates may withdraw consent;
existing approvals and immutable decision history are retained. The user ID is
always taken from authentication; repeated submissions reuse the unique record.

The existing owner dashboard links to home-scoped `/discovery` and
`/discovery/<waiting_id>`. Stage 1 setup remains a stopping page. Discovery is
paginated, searchable by the two approved fields, consent-filtered and limited
to legitimate current home owners. Ordinary active staff cannot review users.

Approval requires explicit recognition, creates/reuses the person-home row,
and records positive approval and an audit event atomically. Existing lifecycle,
roles and review fields are not changed. Association-only rows fail all existing
operational gates. Names never cause automatic matching/approval.

Not ours creates an audit event, not a membership. It affects only this home.
The owner can explicitly reconsider from the home's dismissed list while consent
is current. Profile edits do not reopen decisions. Approved associations cannot
be revoked through discovery. Other homes' history is never returned.

Decisions lock/recheck reviewer membership and candidate consent. Version checks,
database uniqueness and atomic commit prevent duplicate/conflicting approvals.
Identical immediate retries are idempotent; stale conflicting decisions and
lock-timeout/deadlock/serialization conflicts return 409. All mutations use CSRF.

## Local verification

- `tests/retire/check_waiting_transition.py`: creates four controlled homes and
  eight memberships spanning all legacy statuses; compares all prior values
  before/after transition and repetition; removes only those fixtures.
- `tests/retire/check_waiting_room_local.py`: real PostgreSQL request/security
  tests, including a competing request on a separate connection. Most fixtures
  and audit events use request savepoints inside an outer test transaction that
  is rolled back; immutable history is never deleted to clean up tests.
- Existing `check_stage1_onboarding.py`, `check_stage2.py` and
  `check_stage2_local.py` remain regression suites. The obsolete Other placeholder
  assertion now expects the authenticated Waiting Room redirect. Historical model
  parity compares the legacy projection; current schema is verified separately.

Tests use an isolated Flask shell with the actual Retirement blueprint, models,
templates, login and CSRF. Full platform startup is deliberately not invoked
because of known unrelated startup database mutations. No production deployment,
production database change, commit or push is included.
