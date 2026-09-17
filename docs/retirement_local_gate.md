# RCM local integration gate

## Classification of the reported failures

No reported failure required a production-code correction. Counts cover 15
distinct failures: A=0, B=10, C=5, D=0. Counting the duplicated imported failures
in the two runners gives 27 occurrences: A=0, B=19, C=8, D=0.

| Failure | Class | Minimal resolution |
|---|---|---|
| Stage 1 existing operations | C | Owner fixture now includes its approved owner-role relationship |
| Stage 1 setup owner check | C | Same fixture repair; verify revoked-owner status redirect denies setup |
| active_staff_cannot_review | B | Retire legacy-only Staff fixture expectation; current Staff denial covered in Stage 3 |
| approve_records_server_reviewer_and_role | B | Retire closed legacy approval workflow; new audit/actor tests retained |
| cross_organisation_review_is_hidden | C | Owner fixture repair; cross-home 404 check retained |
| deny_clears_approved_role | B | Retire legacy role-decision mutation expectation |
| entry_resolves_existing_membership | C | Owner fixture repair; owner entry check retained |
| join_cannot_request_owner | B | Retire closed legacy join workflow; new owner-role rejection tests retained |
| join_ignores_injected_identity_and_approval | B | Retire closed legacy join workflow; current forged-identity tests retained |
| operational_access_rechecks_membership | C | Owner fixture repair; owner revocation checks retained |
| owner_role_and_unknown_role_cannot_be_approved | B | Retire legacy role-review route expectation; current role validation tests retained |
| repeated_review_fails | B | Retire old mutable membership review; current stale/repeated decision tests retained |
| self_review_fails | B | Old review POST is closed; no current authority is created there |
| templates_compile_and_screens_render | B | Expect redirect from closed legacy join entry; retain template loading |
| Legacy PostgreSQL workflow stopped at join | B | Remove superseded workflow from current gate; use real Waiting Room/Stage 3 PostgreSQL suites |

Stage 1's loader now selects its own eight tests rather than rediscovering the
imported Stage 2 test class. Eight superseded legacy unit tests are explicitly
marked retired, rather than weakening current application authority checks.

## Current integrated set

- `check_stage1_onboarding.py`: 8 current tests.
- `check_stage2.py`: 21 retained tests; 8 obsolete workflows explicitly skipped.
- `check_waiting_room_local.py`: 21 PostgreSQL tests.
- `check_relationships_local.py`: 25 PostgreSQL/security tests.
- `check_full_app_local.py`: actual application factory and local request smoke.

The old `check_stage2_local.py` historical workflow is not a deployment gate for
Stages 1-3: it intentionally expects closed join/role-approval routes to mutate
legacy membership authority. Its replacement coverage uses the actual current
Waiting Room and relationship services with PostgreSQL.

## Actual local application isolation

`SKIP_AUTO_MIGRATE=1` guards only the Alembic call. The actual factory separately
performs unrelated ALTER/UPDATE statements, catalogue updates and an unguarded
CREATE OR REPLACE VIEW. Request hooks also write unrelated visit telemetry.

No application startup code was changed. `check_full_app_local.py` provides a
test-only SQLAlchemy guard and read-only PostgreSQL connections during factory
construction. Startup mutation statements are suppressed before execution (34
on the observed run). Actual models, blueprints, authentication user loader,
request hooks, templates and Retirement services remain loaded.

For smoke requests, only Retirement DML is allowed, inside an outer local
transaction with request savepoints. Unrelated telemetry writes are rejected.
All Retirement fixtures and their immutable history are removed by rollback,
not by disabling audit protections or deleting audit events.

The initial sandboxed factory startup encountered a filesystem PermissionError.
The same protected local smoke passed when execution permission was granted.
No production connection or data was accessed.

Observed smoke passed: About/Owner/create/setup/management; About/Other/Waiting
Room; home-specific Not ours and approval; association without Staff access;
Staff plus multiple roles; Staff withdrawal closing roles; Resident and Family
without Staff roles; cross-home management denial. No Stage 4 features added.
