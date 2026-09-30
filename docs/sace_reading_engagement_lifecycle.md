# Reading/LITRE endorsement engagement lifecycle

Local implementation only. No production cutover has been executed.

## Authority

`access.is_controller()` requires authenticated identity, the exact referenced
SACE operational subject-admin grant, an active controller appointment and an
active Reading engagement. User ID establishes historical identity; the grant's
current email and subject must also match. Enrollment, platform roles and pledge
history cannot confer R authority.

The security journey from commit 80fd9152 remains in place: 15-minute signed-session
context, nonce-bound pledge and explicit authentication continuation, identity
binding and durable nonce consumption. Ordinary login discards abandoned context.
Unknown existing grants require reviewed cutover and are never adopted implicitly.

New Reading tables:
- `sace_reading_engagement`
- `sace_reading_controller_appointment`
- `sace_reading_assignment_context`

Auditor invitations and evidence retain their existing interaction IDs and rooms.
The new assignment context records engagement and issuing appointment. Examination
requires a Claimed, unexpired assignment in an active engagement. Completed Auditors
cannot restore protected access through login or the completion endpoint.

## Lifecycle controls

`/sace/provisioning/lifecycle` exposes active engagement appointments, successor
invitation and explicit completion/revocation controls. POSTs remain CSRF-protected.
Successor links are email-bound, single-use and valid for 24 hours; the successor
must complete the existing pledge/authentication journey. Issuing an invitation
alone does not end the outgoing appointment. Ending an appointment revokes its
unused invitations but does not end already-claimed Auditors or the process.

Closing a process ends active appointments, removes only their exact operational
grants, revokes outstanding/unfinished Auditor assignments and handover invitations,
and retains completed assignments and all history. All protected requests check
current database state. Historical inspection is explicitly available to platform
security administrators at `/admin/security/sace-engagement/<id>`; former R/A
identity alone does not provide historical access.

All lifecycle writes use the subject row as the first transaction lock. Appointment
retirement, process closure and assignment claiming follow the same lock order.
There is one active Reading R appointment per user. No HOME entities are reused.

## Migration and deployment boundary

Revision `reading_sace_001`, standalone `reading_sace` branch, adds only the three
new tables, their indexes and constraints. It changes no existing table or rows.
Downgrade removes only those three new tables; it is destructive to new lifecycle
history and is not a production rollback procedure after cutover.

Existing schema tables must exist first. The repository already has multiple
migration heads and application startup migration/maintenance behavior. Deployment
must coordinate named schema upgrade, reviewed data cutover and code activation;
never run a generic upgrade of unrelated heads or start this code against a schema
without these tables. Unlinked legacy grants/assignments fail closed after activation.
Rollback to grant-only authorization would weaken the security boundary.

## Reviewed cutover service

`lifecycle.reviewed_cutover(manifest, reviewed_by_user_id)` has NO HTTP endpoint,
startup invocation or automatic migration call. It does not commit. An explicitly
reviewed operator transaction must commit or rollback it. The manifest requires:
`reference`, `r_provisioning_event_id`, `r_pledge_event_id`,
`auditor_assignments`, and `reason`. Each assignment explicitly supplies
`invitation_id`, `auditor_user_id`, and its reviewed `status` (Claimed, Completed
or Revoked). Duplicate invitation IDs, mismatched users/statuses and already
linked invitations are rejected. Only listed assignments are linked; their source
payloads, states, timestamps and evidence remain unchanged. Unclaimed invitations
are not supported by this reviewed claimed-assignment cutover.

The reviewed R treatment remains grant 2 for 622; grant 3 for 630 is retired.
Production SELECT evidence supplied by the operator identifies subject 44,
622 provisioning/pledge events 72/73, and 630 test events 304/305. The reviewed
Auditor list is:

```json
[
  {"invitation_id": 74, "auditor_user_id": 623, "status": "Claimed"},
  {"invitation_id": 218, "auditor_user_id": 630, "status": "Claimed"}
]
```

630's legitimate earlier Auditor assignment and evidence are distinct from the
later test-created R grant. Cutover creates no R appointment for 630 and preserves
user/enrollment records and both histories. No other grant or assignment is
inferred legitimate. Source IDs and mappings are validated again at execution.
The fixed manifest reference, reason and identified reviewing actor still require
operator selection before production execution. No production cutover has run.

The manifest hash/reference make an exact repeat idempotent; different provenance
is rejected. Exact repeats return the existing engagement without revalidating
subsequent lifecycle changes. Local tests use production event/assignment IDs with
synthetic data in isolated local tables, not production records.

## Focused verification

- `python -B tests/support/sace_reading_lifecycle_runner.py`
- `python -B tests/support/sace_access_postgres_runner.py`
- `python -B tests/test_sace_journey.py`
- `python -B tests/support/sace_reading_repair_runner.py`
- `python -B tests/support/sace_reading_home_isolation_runner.py`

Database harnesses require verified localhost `ait_local_db`. Most data is held in
connection-local temporary tables. The concurrent-transaction test uses uniquely
prefixed local fixture tables exposed through worker-local temporary views, and
removes only those fixtures afterward. Migration round-trip runs in temporary
tables and rolls back. No platform app factory or production configuration is used.

Two obsolete Demo tests in the access runner still expect the retired intro/old
sequence and are intentionally unchanged. HOME source/tests are unchanged; the
Reading isolation wrapper supplies lifecycle-valid Reading R fixtures for two
older tests that assumed a bare subject-admin grant was sufficient.
