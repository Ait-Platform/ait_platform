# Retirement Stage 2 implementation ? source only

No database connection, provisioning, migration chain, Alembic stamp/version change, deployment or production access was performed. These definitions are proposed only. Do not run `flask db upgrade` or this historical chain. Direct local provisioning requires a separate approved plan.

## Files changed in this task

- `app/models/retire.py`
- `app/models/__init__.py` (Retirement exports only)
- `app/program_retire/routes.py`
- `migrations/versions/retire_02_membership.py`
- `templates/program_retire/welcome.html`
- `templates/program_retire/about.html`
- `templates/program_retire/dashboard.html`
- `templates/program_retire/entry.html` (new)
- `templates/program_retire/join.html` (new)
- `templates/program_retire/status.html` (new)
- `templates/program_retire/pending.html` (new)
- `templates/program_retire/review.html` (new)
- `tests/retire/check_stage2.py` (new)
- `docs/retirement_stage2.md` (this report)

Existing unrelated work was preserved. UIP, core organisation logic, Loss and WeasyPrint were not edited.

## Exact proposed PostgreSQL schema

Compiled offline from the proposed migration definitions. No SQL below has been executed.

```sql
CREATE TABLE retirement_role (
	id SERIAL NOT NULL, 
	code VARCHAR(64) NOT NULL, 
	name VARCHAR(100) NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (code)
);

CREATE TABLE retirement_membership (
	id SERIAL NOT NULL, 
	organisation_id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	requested_role_id INTEGER, 
	approved_role_id INTEGER, 
	status VARCHAR(16) NOT NULL, 
	requested_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
	reviewed_at TIMESTAMP WITH TIME ZONE, 
	reviewed_by_user_id INTEGER, 
	review_reason TEXT, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_retirement_membership_org_user UNIQUE (organisation_id, user_id), 
	CONSTRAINT ck_retirement_membership_status CHECK (status IN ('pending', 'active', 'denied', 'disabled')), 
	CONSTRAINT ck_retirement_membership_active_role CHECK (status != 'active' OR approved_role_id IS NOT NULL), 
	CONSTRAINT ck_retirement_membership_unapproved_role CHECK (status NOT IN ('pending', 'denied') OR approved_role_id IS NULL), 
	FOREIGN KEY(organisation_id) REFERENCES retirement_organisation (id), 
	FOREIGN KEY(user_id) REFERENCES "user" (id), 
	FOREIGN KEY(requested_role_id) REFERENCES retirement_role (id), 
	FOREIGN KEY(approved_role_id) REFERENCES retirement_role (id), 
	FOREIGN KEY(reviewed_by_user_id) REFERENCES "user" (id)
);

CREATE INDEX ix_retirement_membership_org_status ON retirement_membership (organisation_id, status);

CREATE INDEX ix_retirement_membership_user_id ON retirement_membership (user_id);
```

## Validating, idempotent data establishment

The schema above is unchanged. The authoritative data-establishment source is now `establish_roles` and `backfill_owner_memberships` in `migrations/versions/retire_02_membership.py`. The old unconditional seed/backfill SQL is superseded and must not be used.

Canonical code/name pairs:

| Code | Name |
|---|---|
| organisation_owner | Organisation Admin / Owner |
| facility_manager | Facility Manager |
| care_staff | Care Staff |
| administration_reception | Administration / Reception |
| finance | Finance |
| kitchen_catering | Kitchen / Catering |

`establish_roles(connection, roles)` looks up each canonical code with a row lock. Missing codes are inserted with their exact canonical names using PostgreSQL `ON CONFLICT (code) DO NOTHING`, then read back and validated. Matching records and IDs are preserved without an update. Any name mismatch raises `ProvisioningConflict`; no overwrite or rename is performed. IDs are resolved from the stored records, never fixed constants. On a repeat against matching data, no inserts are attempted.

`backfill_owner_memberships(connection, organisations, members, owner_role_id)` receives the owner role ID returned by validated role establishment. For every organisation, it checks the authoritative owner's membership and any membership assigned the owner role. A non-owner holding that role causes failure. A missing owner membership is inserted with active status, the validated owner role, NULL requested role and NULL review metadata; `requested_at` uses its unchanged database default. Insertion uses `ON CONFLICT (organisation_id, user_id) DO NOTHING` and revalidates the resulting state. Existing owner memberships must have the authoritative user, active status, matching owner role and NULL requested role. Conflicts fail without repair. Matching records, including timestamps and review metadata, are preserved. Unrelated staff memberships are not changed. Repeats insert no duplicates. An empty organisation table produces no membership writes.

Both helpers require a caller-owned transaction. They do not commit or swallow exceptions. ANY conflict or other failure requires rollback of the entire transaction, including earlier inserts. Conflict-safe insertion never overwrites a competing row: the row is read and validated afterward. Row locks are held until the caller completes its transaction. These guarantees do not replace the future local preflight or live PostgreSQL testing.

Only data establishment is idempotent. `upgrade()` still creates the tables and indexes once; do not rerun it against existing tables, invoke `flask db upgrade`, or replay the historical migration chain. A future authorized controlled local provisioner must establish/validate schema separately and call these helpers in one transaction, rolling back on any exception.

## Dependencies and comparison

Source dependencies validated: `app/models/auth.py` defines integer primary key `user.id`; Stage 1 model/migration define `retirement_organisation.id` and its required unique `owner_user_id` foreign key to `user.id`. No core or UIP foreign keys exist in Stage 2. PostgreSQL timezone-aware timestamps are used; `requested_at` defaults to current time in both model and database definitions. Other nullable fields have no server defaults; status has no server default and is explicitly assigned by trusted routes.

The offline check compared model/migration column names, types, nullability, primary keys, server defaults, foreign-key targets, unique constraints, check expressions and indexes. All match. Six migration role seeds match the application role definitions. Mapper configuration and Retirement imports passed in an isolated package environment.

Actual local database structure, existing owner rows, foreign-key validity and database privileges remain unverified: no database was connected. They must be checked before proposing or approving direct local SQL.

## Route flow

All routes below use `/retire` as prefix. Authenticated `/` redirects to `/entry`; unauthenticated `/` remains the welcome page. `/entry` requires login and resolves a single membership to its dashboard or status. With multiple memberships it shows an organisation selector; with none it offers join or registration. Existing `/dashboard` URLs redirect to `/entry`.

- `/join`: enter a Retirement organisation ID supplied by its owner.
- `/organisations/<organisation_id>/join`: display and confirm the organisation, choose a staff role and submit a pending request. Repeated submissions resolve the existing membership without resetting its status.
- `/organisations/<organisation_id>/status`: pending waiting room or denied/disabled status with review reason and no operational access. Active members redirect to dashboard.
- `/organisations/<organisation_id>/dashboard`: active membership required on every request. All ten functional tiles remain placeholders, without role-to-tile mapping.
- `/organisations/<organisation_id>/members/pending`: only the active authoritative owner sees pending requests for this organisation.
- `/organisations/<organisation_id>/members/<membership_id>/review`: owner review, approve with a permitted staff role or deny; a nonblank review reason is required.
- `/register`: register an organisation and establish its active owner membership in a single transaction.

Returning active staff go directly to their dashboard for a single membership; multiple memberships require selection. Later logins do not trigger another approval. There is no disabled/denied reapplication or reactivation flow in this stage.

## Security and transaction behavior

Applicant identity always comes from `current_user.id`. Submitted user IDs, status, approved roles, reviewer IDs and timestamps are not bound to membership creation. Forms use CSRF protection. Join and review role choices are limited server-side to the five staff codes, with an additional explicit code check before assignment. The owner role is never available through these flows.

Protected operational routes use `active_membership_required`, which queries membership on each request and requires both active status and an approved role. This gate can be composed with future capability checks; it does not equate roles with tiles. Pending, denied and disabled memberships receive only their status page.

Review additionally verifies `retirement_organisation.owner_user_id == current_user.id`, active membership in that organisation, a target membership in that same organisation, a different applicant user and pending status. A row lock prevents concurrent decisions from silently overwriting each other; completed reviews return conflict. Reviewer ID and UTC review time are server-assigned. Denial clears the approved role. Database constraints enforce unique membership and required status/role invariants even outside these routes.

Registration adds and flushes the organisation, adds the active owner membership using the seeded owner role, then commits once. Requested role, reviewer ID and review time are NULL. Integrity failure rolls back both records; repeated concurrent registration resolves an already-created owner membership. Any other exception in the registration write/flush/commit block now explicitly rolls back before re-raising the original exception. There is no core organisation creation. Existing owners missing membership fail closed pending the proposed backfill, without an implicit repair on GET.

## Verification and limitations

Run `.venv\Scripts\python.exe tests/retire/check_stage2.py` directly. Twenty-nine database-free checks passed, including all original seventeen checks: isolated imports/mapper setup, Python syntax, template compilation/rendering, schema parity, server-owned applicant/reviewer data, forbidden roles, cross-organisation and self-review, non-owner rejection, repeated reviews, active membership gating, entry resolution, denial, CSRF rejection and registration transaction/rollback behavior.

Additional cases cover missing/matching/conflicting roles and owner memberships, repeated establishment without writes or duplicates, preservation of unrelated staff, non-owner ownership conflicts, empty-organisation no-op, and unexpected registration failures during membership add, flush and commit.

Checks use mocked persistence and offline SQL compilation. They do not verify PostgreSQL execution, actual concurrent transactions, the full platform factory or browser layout. No engine or SQLite database was created. Stage 2 functionality will require the reviewed schema and seeds before it can run against the application database. Work stops here before provisioning.

## Source-only hardening task

Files changed: `migrations/versions/retire_02_membership.py`, `app/program_retire/routes.py`, `tests/retire/check_stage2.py`, and this report. No model, schema definition, foreign key, timestamp definition, constraint, index, role code, status, or other program was changed.

READY FOR LOCAL STAGE 2 PREFLIGHT: YES

Remaining prerequisite: a separately authorized inspection must confirm the actual local database identity (`ait_local_db`), dependency columns/constraints, current Stage 2 objects/data and permissions. This task did not inspect any database, execute SQL, provision tables, alter Alembic state or deploy. Historical migration reconciliation remains parked.
