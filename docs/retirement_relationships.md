# RCM Stage 3: relationship and Staff authority

Local implementation only. No production deployment, commit, historical Alembic
replay, migration stamping, payment change or application-wide startup is included.

## Current authority

`retirement_membership` remains the person-home identity. Positive association
approval is required before relationship grants. Current unwithdrawn relationship
periods establish Staff, Resident or Family / Representative independently.
Staff eligibility additionally requires a current unwithdrawn assignment of one
of the five existing operational Retirement roles. The existing owner role is
never a Staff role. Resident and Family need no Staff role and gain no resident
data or operational module through this stage.

Founding Owner/Admin authority is checked separately against actual ownership
and the legitimate owner membership. Facility Manager does not imply admin.
Legacy `approved_role_id` is no longer a source of Staff eligibility. Existing
legacy fields and review history remain unchanged, including after withdrawal.
Legacy join/role-review POST routes return 409; historical review remains readable.

## Schema

- `retirement_relationship`: membership FK, one grant period with kind,
  grant actor/time, recorded time, provenance, optional unique legacy-source
  membership FK, and optional withdrawal actor/time/reason.
- `retirement_staff_role_assignment`: relationship and existing role FKs,
  grant/withdrawal facts and provenance as above.
- `retirement_authority_event`: immutable relationship/assignment audit,
  membership FK, actor, action, recorded time, reason and per-membership revision.
  Import events are explicitly labelled and have no fabricated execution actor.
  Source grant records retain the evidenced historical reviewer and timestamp.

Partial unique indexes permit only one current membership/kind and one current
relationship/role. Unique legacy-source keys prevent repeated import or revival.
Checks validate kinds, origins, attribution, withdrawal chronology and events.
Database triggers reject history deletion, truncation and grant-history rewriting;
only initial withdrawal fields may be set. Cross-table guards require approved
association, current Staff parent and operational role, matching provenance and
matching event parentage. Closing a relationship with active roles is rejected.

Authority events supply immutable history and optimistic concurrency revisions,
not current permissions. Runtime authority does not replay events.

## Mutations and UI

Home Owner/Admin can explicitly grant/withdraw relationships and Staff roles.
Requests derive actors from authentication, scope targets to the home, require
CSRF, lock authority records, and compare a revision. Stale/concurrent conflicts
return 409. Duplicate current grants create neither extra periods nor events.
Pending/denied/disabled legacy decisions require explicit acknowledgement before
a new grant; they are not automatically converted or overwritten.

Staff withdrawal closes every active child assignment, appends each audit event,
and closes Staff in one transaction. Resident and Family are untouched. A fresh
Staff grant starts without roles. Rollback restores all records on failure.

The owner dashboard links to a minimal home-scoped relationship management page.
Grant controls and per-record withdrawal buttons are separate from Stage 1 setup.
No Acting-as, context selector, final dashboard routing or operational modules
have been introduced.

## Local transition and checks

`scripts/retirement_relationship_transition.py apply` positively verifies local
`ait_local_db` and the Stage 2 schema, inventories active legacy authority before
cutover, then transactionally creates/verifies the scoped schema and imports
sufficiently evidenced operational grants. Unresolved active authority blocks
the transaction. Owner records are not converted into Staff. Pending, denied and
disabled rows are not imported. Existing association facts are preserved; absent
facts may be filled only from the same trustworthy source evidence.

Local initial inventory: no legacy memberships, so zero real records imported.
Focused fixtures verified evidenced import, repeat no-op, unchanged source fields,
insufficient-evidence rejection and no revival after withdrawal.

Run only `tests/retire/check_relationships_local.py` for this stage. Its explicit
test loader selects Stage 3 only. It reuses the local Flask/PostgreSQL transaction
harness, without executing the application factory or broad suites. Immutable
test history is cleaned by rolling back its creation transaction, never deletion.
The separate-connection concurrency test cleans only its own committed base
fixtures after decision rollback.

The initial runner inadvertently discovered imported Stage 2 tests; selection
was corrected. Two obsolete Stage 2 assertions (no relationship table, and legacy
active role alone grants access) are intentionally left for the later integrated
validation update. They are not Stage 3 authority rules.

Focused result: 25 tests passed. Schema repeat application verified no-op. Full
integrated Retirement validation remains required before production.
