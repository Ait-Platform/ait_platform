# Isolated UIP PostgreSQL regression suite

The current local development target is `uip_p49_operations`, descending from
`uip_p3_work_orders`. Production has not been migrated by this implementation.
See `../../UIP_PHASE4_9_IMPLEMENTATION.md` for the consolidated feature, schema,
permission, validation and restored-production rehearsal handoff. The latest
test fixture applies Phase 4–9 inside the same disposable-schema safeguards.

Run from the repository root in a separate Python process:

```powershell
$env:UIP_TEST_DATABASE_URL = 'postgresql+psycopg2://TEST_USER:TEST_PASSWORD@127.0.0.1:5432/uip_test_local'
python -B -m pytest --confcutdir=tests/uip tests/uip -q -p no:cacheprovider
python -B tests/test_startup_enrollment_preservation.py
```

Use an explicitly local PostgreSQL database owned by the test user. The harness
rejects remote hosts, SQLite, URL query overrides and database names other than
`ait_local_db` or `uip_test_*`. It creates and drops only its generated
`uip_test_<uuid>` schema. It does not use DATABASE_URL or import the root pytest
conftest. Each test rolls back its transaction, including route commits.

The harness imports the actual UIP/core models, blueprint, services and templates
without importing the shared application factory. Only User and the unrelated
token-transaction FK target are minimal test models. A separate offline subprocess
loads the real application model registry and forbids factory execution and any
SQLAlchemy database connection. CSRF and login checks are enabled.

The Phase 2 migration has a dedicated root: `uip_p2_prod_base` (branch `uip`).
Its ancestry is only `uip_p2_prod_base -> a27c9e4b6102`; the legacy head
`7da57fffdba9` remains separate and unchanged. The validation-only bridge requires
online PostgreSQL catalog inspection. It checks frozen UIP table/column types,
keys and tenant/actor foreign keys, required nullability, unique references,
absence of Phase 2 objects, empty migration tracking, and unexpected triggers.
It deliberately preserves nullable membership `is_active`, rejecting NULL values
for separate review instead of repairing records. Offline execution of the bridge
is refused because it cannot validate the database.

No local production snapshot was available. `test_bridge.py` reconstructs the
inspected UIP prerequisite state in a disposable schema, including the known
nullable/default/FK differences. It is not a complete production clone. Protected
SACE sentinel tables mirror the local SACE model. Tests run the actual Alembic
graph, compare all existing rows and untouched structures, and verify exactly the
six added Phase 2 tables and three interaction columns. Existing migration tests
also cover model parity, empty Phase 2 downgrade/re-upgrade and refusal of populated
downgrade. Baseline downgrade refuses to erase the adopted schema/history.

After separate backup and migration approval, run only the explicit target:

```powershell
python -B -m alembic -c alembic.ini upgrade a27c9e4b6102
```

Never use `upgrade head`, `stamp`, or execute the legacy chain. The graph intentionally
has two heads. A production restore rehearsal is still recommended before release;
no production database is accessed by these tests.

For a separately approved migration, use Alembic directly from the repository root
(`python -m alembic -c alembic.ini ...`) with DATABASE_URL explicitly set.
Do not use `flask db ...`: Flask CLI constructs the app before Alembic can load
env.py, so it would still execute normal shared startup maintenance. The env.py
change removes the factory call from direct Alembic execution only; it does not
change normal application startup or analytics.


## Phase 3 local validation

Phase 3 target: `uip_p3_work_orders`, parent `a27c9e4b6102`. Its ancestry is
`<base> -> uip_p2_prod_base -> a27c9e4b6102 -> uip_p3_work_orders`.
The legacy branch is unchanged. After a separately approved restored-production
rehearsal, backup and live migration approval, the scoped command would be:

```powershell
python -B -m alembic -c alembic.ini upgrade uip_p3_work_orders
```

This migration refuses every existing work order and unexpected Phase 3 objects.
It does not infer capabilities, provider associations, or dispatch history. Existing
providers start with UNKNOWN availability; operational eligibility requires manual
manager configuration. Downgrade is refused to protect history. The production
membership is_active nullable mismatch remains untouched.

The tests use frozen synthetic Phase 1/2 SQL fixtures, separate Phase 2/latest
schemas, and actual migrations. Concurrent tests use independent PostgreSQL
sessions with committed synthetic records in additional disposable UUID schemas.
No production URL, backup, restore or production data is used by this suite.

Work orders progress CREATED -> DISPATCHED -> ACCEPTED -> IN_PROGRESS -> COMPLETED
-> VERIFIED -> CLOSED. Verification rejection returns COMPLETED to IN_PROGRESS.
Provider rejection (from DISPATCHED), failure (from ACCEPTED/IN_PROGRESS), and
manager cancellation (from CREATED/DISPATCHED/ACCEPTED/IN_PROGRESS) are terminal
and leave the issue open. CLOSED atomically resolves the issue only after manager
verification and no outstanding actionable CoreTask. No parallel task system is added.

Task completed is terminal. Task cancelled is non-actionable only with recorded
UIP actor, cancellation time and reason. Unknown/NULL and unsubstantiated historical
cancelled statuses remain actionable. Cancellation retains the original task and
creates a UIP audit event in the same transaction. Provider-only users cannot
complete/cancel internal tasks or access general issue/register screens.
