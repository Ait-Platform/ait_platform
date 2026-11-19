# Municipal account uniqueness repair

## Confirmed cause and production findings

The lookup already scopes accounts to the authenticated property's manager through
`bil_property.manager_id`. An unassigned legacy account is visible only when its
meter pointers positively and unambiguously establish that manager's ownership.
`owner_id` identifies a landlord, not the property manager.

The supplied production results identify account 1 (`70134280012`) with no property,
no meter pointers and owner 3. Target property 27 belongs to manager 644. Lookup
correctly hides account 1, but PostgreSQL's global
`bil_muni_account_account_number_key UNIQUE(account_number)` rejects an independent
INSERT. The old catch-all constraint handler returned 503.

No production connection was used locally. The subsequent production inspection
reported zero rows in all eight FK dependencies and both number-only history checks
for account 1. These findings were supplied by the operator. Keep the read-only
`scripts/inspect_pm_municipal_history.sql` results with deployment approval.
Number-only history matches cannot establish ownership.
Do not claim, transfer, delete or edit account 1.

## Application and migration behavior

Duplicate saves return a validation response (409) on the same Municipal Accounts
page, preserving entered values. Existing edits retain their optimistic edit token
and server-resolved identity checks; no unverified overwrite is introduced. Lookup and permission
rules stay unchanged. Before migration, the legacy global constraint produces an
explicit deployment-required validation message rather than 503.

Prepared forward-only revision:
`pm_mar_payment_reference_001 -> pm_muni_manager_unique_001`.

The migration adds no ownership columns or tables and changes no account or history
rows. Three triggers derive manager scope from existing property/meter ownership:

- Reject normalized (trimmed, case-insensitive) duplicate numbers in one manager's
  records, while allowing identical numbers for different managers.
- Retain unverified legacy accounts in an unassigned uniqueness scope; this does
  not grant anyone access to them. A verified meter-linked legacy account follows
  its existing manager. Ambiguous legacy ownership remains unassigned.
- Reject property-manager or meter-property changes that would violate uniqueness.

Under table locks, the migration checks existing conflicts, installs and verifies
the replacement triggers, then drops the old global constraint as its final step.
Any normalized same-scope conflict aborts for authorised review; nothing is merged.
Local conflict fixtures pass; full production conflict status remains unknown until
preflight runs. Do not resolve conflicts by changing live data automatically.

Because PostgreSQL cannot put a cross-table manager lookup in a unique constraint,
the triggers serialize municipal identity changes using a transaction advisory lock.
They require READ COMMITTED (PostgreSQL/SQLAlchemy's normal default) and reject
REPEATABLE READ / SERIALIZABLE identity changes rather than risk stale snapshots.
This serializes account identity and ownership edits; it does not lock ordinary
read-only reporting. The migration takes SHARE ROW EXCLUSIVE locks on property and
meter tables (allowing ordinary reads, preventing ownership writes and supporting
CREATE TRIGGER). Only the account table takes ACCESS EXCLUSIVE, required to drop
its old unique constraint. It is acquired before preflight to avoid a late upgrade.
The five-second lock timeout and 60-second statement timeout remain unchanged.
Schedule a quiet period. A timeout or failure rolls back the entire migration.

### Lock-timeout review

The first production attempt timed out on the original three-table ACCESS EXCLUSIVE
lock. That proves lock contention, not the identity or state of a blocking session.
Ordinary open read transactions could block that original request. The revised
locks avoid this on property/meter tables, but active writers still block them and
any account-table reader/writer transaction can block the necessary account lock.
This is not a fully online migration and does not guarantee a successful busy-time retry.

Run `scripts/inspect_pm_municipal_locks.sql` in authenticated PostgreSQL for current
holders/waiters, blocker PIDs and transaction ages. The script performs only reads,
does not print user query text, and never cancels or terminates sessions. After a
timeout, the vanished wait cannot be reconstructed from current activity alone.
Review any idle-in-transaction holders with their owning application/operator.
Let them finish or explicitly commit/roll back their work; do not kill sessions
automatically. Do not hold a diagnostic transaction open across the retry.

Before a separately approved retry, confirm the failed transaction has rolled back:
the PM head must still be `pm_mar_payment_reference_001`, and the legacy global
constraint must still exist. Deploy the revised migration, take the normal backup,
and arrange a short quiet/maintenance window for affected PM requests, allowing
in-flight transactions to drain. Run the existing one-line command below only after
approval. If contention persists, stop and inspect blockers; do not increase the
timeout blindly or split the replacement and constraint drop across transactions.
No account/history records, financial calculations or Render settings are changed.

## Deployment procedure — approval required, not executed

1. Deploy the verified commit. Do not use an automatic `upgrade heads`: approval
   is required and the migration deliberately rejects an unapproved context.
2. Review the read-only history results and current Alembic heads. Confirm the
   production PM head is `pm_mar_payment_reference_001`; if not, stop and review
   prerequisite migrations separately. Verify database and role against Render's
   current resource configuration without printing credentials. Take the normal
   database backup and obtain explicit approval for this revision.
3. In authenticated Render Shell, use the service's existing `DATABASE_URL`.
   After approval only, run this **single-line** command:

```bash
python -c "from alembic import command; from alembic.config import Config; c=Config('migrations/alembic.ini'); c.attributes.update(skip_app_metadata=True, pm_muni_manager_unique_approved=True); command.upgrade(c,'pm_muni_manager_unique_001'); command.current(c)"
```

This uses standard Alembic and its PostgreSQL transaction, avoids the Flask factory,
and upgrades only the targeted PM branch. Other Alembic heads are preserved. Never
stamp or manually edit `alembic_version`. On any failure, stop, review the error,
and confirm the old constraint/revision remains; do not retry with checks removed.

4. Read-only verification:

```sql
SELECT version_num FROM alembic_version ORDER BY version_num;
SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid='bil_muni_account'::regclass AND contype='u';
SELECT tgrelid::regclass,tgname,tgenabled FROM pg_trigger
WHERE NOT tgisinternal AND tgname IN
 ('pm_muni_account_unique','pm_muni_property_scope_unique','pm_muni_meter_scope_unique');
SELECT id,property_id,owner_id,water_meter_id,elec_meter_id
FROM bil_muni_account WHERE id=1;
```

Expect the new PM revision, unchanged other heads, the legacy global constraint
absent, three enabled triggers (`O`), and account 1 unchanged. Re-run history counts
and compare with preflight. Use the normal authenticated UI for the independent
property-27 registration; no manual INSERT or ownership reassignment is needed.

## Local verification

`tests/test_pm_muni_manager_uniqueness.py` uses session-temporary PostgreSQL tables
in verified localhost `ait_local_db`, never production or persistent test records.
It exercises the actual migration DDL, success/rollback, normalized conflicts,
separate managers, legacy preservation, lookup, edits and cross-manager rejection.
No production migration or production financial record has been changed.

Original verification results: nine new focused cases pass. The expanded municipal suite
has 50 passes and five failures: three opening-reading/schema-readiness assertions
and two submenu assertions expecting the old menu without Output. Re-running these
five with the unchanged HEAD `save` and `page` functions reproduces all five failures.
They are outside this correction and remain unchanged. Alembic's 16-head graph
resolves the new revision and its declared parent. The optional JavaScript lookup
test could not run because Node.js is unavailable in this environment; no JavaScript
or templates were changed.

Lock correction verification: all ten focused migration/uniqueness tests pass,
including a PostgreSQL `pg_locks` assertion that property/meter tables hold
ShareRowExclusiveLock without AccessExclusiveLock, and that the account table
holds AccessExclusiveLock. Existing atomic DDL rollback, duplicate conflict,
legacy-account preservation and isolation tests still pass. These session-temporary
fixtures verify actual lock modes, not live production contention or a multi-session
load test. No second production migration attempt was made.
