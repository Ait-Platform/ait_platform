# Owner persistence implementation and activation

Application implementation is independent of the migrations in preparation commit
3df9ee56. Do not merge/deploy that branch or apply its migrations without production
preflight, verified backup, restored-database rehearsal and explicit approval.

## Default-off behavior

Config parses the PM_OWNER_WRITES_APPROVED environment setting into a Boolean.
It is absent/false by default. Set PM_OWNER_WRITES_APPROVED=true only after explicit
administrator activation approval and verified schema/backup/rehearsal. Invalid
values remain false. This integration does not set any production environment variable. When false,
owner APIs return unavailable without touching owner tables; property registration,
address editing and the dashboard retain their existing behavior. The interface remains
a clearly labelled browser-only preview with Save Owner and existing-owner search disabled.

Even when the approval flag is true, activation requires all four ownership tables,
expected columns, pm_owner_002 in alembic_version and enabled ownership validation,
immutability and no-truncate triggers. Approval must only be supplied after external
migration verification; this code cannot certify backups or approval by itself.

## Enabled behavior, locally tested

Save Owner locks/rechecks the manager's property, creates or reuses an accessible
identity, adds the property-owner association, reads current owners and commits one
transaction. Failed association/commit rolls back both records; success is returned
only after commit. Existing IDs are reused. New identities require an ID/registration
number for safe manager/type-scoped deduplication; names never determine identity.
An existing identity without a number can still be selected rather than recreated.
Postal-same is enforced on both frontend and backend. Multiple owners are supported.

Owner search is limited to identities created by the manager or explicitly associated
with manager-authorized properties. Shared identity edits are limited to the identity
creator and clearly labelled as affecting all linked properties. They preserve historical
snapshots. Per-property percentages and removals append superseding assignment records,
never delete identities/history or mutate other properties' assignments. Current
assignments are terminal versions whose effective interval includes today (end exclusive).
Removed assignments end today. Requests are authenticated and use the existing global
CSRF protection; JSON requests send X-CSRFToken.

After commit, the modal closes and the Owners cell updates from the persisted response.
The Edit page displays each assigned owner's saved identity/contact/address/share fields.
Shared-detail edits reload that view after success. Failed saves stay in the modal and
show an error, never a success message. No tenants, units, leases, meters or accounts
are created by these operations. No lease/billing programme logic is changed.

## Required production activation sequence

1. Obtain authoritative Render resource connection and run the read-only checker from
   preparation commit 3df9ee56. Verify schema/PK/column types, Alembic ancestry, existing
   data conflicts, privileges and collisions. Missing bil_muni_account.property_id is a
   separate legacy issue and must not be silently fixed by the ownership migration.
2. Create a usable production pg_dump backup, restore to a separate database and verify
   recovery. Rehearse the actual Alembic runner upgrading specifically pm_owner_002,
   including its pm_owner_001 dependency; compare business IDs/values and snapshots.
3. Review rehearsal/rollback results and obtain explicit production migration approval.
4. Apply only the reviewed revision sequence transactionally. Verify schema, triggers,
   constraints, indexes, retained prior Alembic heads and unchanged business records.
5. Only then explicitly enable PM_OWNER_WRITES_APPROVED=True through trusted app
   configuration and verify manager-authorized creation, reuse, multiple owners, edit,
   removal, failure rollback and postal synchronization on the deployed application.

Readiness remains unverified; no production migration, backup or rehearsal is claimed.
Rollback application deployment/disable the flag while preserving owner history. DDL
failures roll back transactionally; destructive automatic downgrade is disabled.

Local integration tests use the separate prepared migration worktree, applying both
revisions only to session-temporary PostgreSQL fixtures. Missing fixtures cause a stated
skip rather than inventing database results. Run with that worktree present:
python -m unittest discover -s tests -p test_pm_owner_operations.py

## Environment activation procedure

After explicit activation approval, set PM_OWNER_WRITES_APPROVED=true on the
verified running Render service and restart/redeploy so Flask reloads configuration.
Leave it unset or false until then. Remove it or set false and restart to disable.
Review instance configuration and FLASK_PM_OWNER_WRITES_APPROVED overrides, which
load after Config; the final Flask value must be Boolean True to enable.
A true approval value does not bypass the schema/revision/trigger checks.
Verify the service database identity and pm_owner_002 revision through its actual
runtime connection, without invoking application startup maintenance for diagnostics.
The administrator report of installed tables is not evidence of Alembic revision
state or of the service connecting to that database.
