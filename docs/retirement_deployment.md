> Naming-refactor review: runtime paths below use program_retire. The existing retirement-stage2-deployment.zip and retirement_integration.patch are historical artifacts for baseline 049c325 / release c2d2388; do not apply them to the renamed tree. Prepare a fresh release only after review of this uncommitted refactor.

# Retirement local verification and deployment manifest

RETIREMENT LOCAL FLOW: PASS

RETIREMENT READY TO DEPLOY: YES

This means the Retirement implementation and selected deployment set are ready for a separately authorized production preflight/provisioning/release task. Production schema and full platform startup have not been tested or changed.

## Local results

The write connection was independently verified as PostgreSQL 127.0.0.1:5432 / ait_local_db / public. Inherited SQLite environment settings were not used. Stage 1, user.id, Stage 2 object absence and Alembic revision were checked immediately before writing.

One transaction created retirement_role and retirement_membership, established six canonical roles and performed the owner backfill (zero organisations, zero backfill rows). Before commit, verification covered every column/type/nullability/default, PK, FK, unique/check constraint, explicit index, both owned integer sequences, role mapping and owner coverage. Check expressions were evaluated semantically; sequence linkage was checked using catalog dependencies rather than textual default equality.

Forty real PostgreSQL route checks passed: registration, owner membership state, ten tiles, pending queue, staff lookup/join, waiting room, duplicate requests, applicant/privilege injection prevention, approved-role allow-list, approval with retained requested role and reviewer/time, returning staff, organisation isolation, non-owner/self/cross-organisation review rejection, denial, disabled access, database uniqueness and CSRF.

The isolated Flask app used real Retirement routes/forms/templates and the shared layout, authenticated sessions over existing local user IDs, and real PostgreSQL commits. It never called create_app(). A minimal user mapping and inert navigation endpoints isolated platform startup/auth-shell behavior. No user records were modified. A missing now() helper in the initial test harness was supplied to match the platform template context; no runtime application change was needed.

Anonymous /retire/ and /retire/about returned 200. The root / endpoint was an inert test shell, so this is not a full AIT root/login smoke test. Existing Welcome Get Started links to /retire/entry; About is available at /retire/about. No navigation redesign was included.

All 29 database-free regression tests passed. Cleanup removed only four memberships and two uniquely named test organisations. Final schema verification passed with retirement_organisation=0, retirement_membership=0, retirement_role=6. Alembic remains add_letterhead_to_sender.

## Exact runtime files

- `app/models/retire.py`
- `app/program_retire/__init__.py`
- `app/program_retire/routes.py`
- `templates/program_retire/about.html`
- `templates/program_retire/dashboard.html`
- `templates/program_retire/entry.html`
- `templates/program_retire/join.html`
- `templates/program_retire/pending.html`
- `templates/program_retire/register.html`
- `templates/program_retire/review.html`
- `templates/program_retire/status.html`
- `templates/program_retire/welcome.html`

Shared integration requires SELECTIVE hunks only:

- app/models/__init__.py: import/export the three Retirement models.
- app/__init__.py: import/register retire_bp. Ensure SKIP_AUTO_MIGRATE=1 gates historical automatic upgrade and any boot-time db.create_all block before release restart. The local working file already guards upgrade and has removed that create_all block. Do not copy its unrelated dirty changes.

The supplied retirement_integration.patch is based on Git HEAD 049c3251bafa6e31bb0524baeaf6fee76dec9489. It adds only Retirement registration/exports and the minimal skip-flag guard around that baseline's migration/bootstrap block. It preserves default behavior when the flag is unset. It was prepared as an artifact, not applied to application source. Adapt these hunks to the actual deployment baseline after inspection. Set SKIP_AUTO_MIGRATE=1 for the release. The flag does NOT disable unrelated startup maintenance.

No auth/routes.py, UIP/core file, other-program file, global template or dependency update belongs in this deployment set. Existing platform authentication and shared templates are prerequisites. A platform program-catalog link, if required beyond direct /retire/ entry, must be checked against existing production configuration separately.

## Exact production schema actions (NOT AUTHORIZED OR EXECUTED)

1. Obtain separate authorization, explicitly verify the production target/public schema and inspect privileges, existing Retirement tables, user.id, owner data and Alembic state. Do not assume production matches local.
2. Validate and preserve retirement_organisation if present. If absent, Stage 1 requires id SERIAL PRIMARY KEY, name VARCHAR(200) NOT NULL, owner_user_id INTEGER NOT NULL UNIQUE REFERENCES public."user"(id). Create it only under explicit production schema authorization.
3. In one controlled transaction, establish the two Stage 2 tables and indexes exactly as docs/retirement_stage2_schema.sql. Unexpected existing objects require semantic inspection and a revised plan; do not conceal schema drift using IF NOT EXISTS.
4. Run establish_roles from migrations/versions/retire_02_membership.py on the same transaction connection. It inserts missing canonical roles, preserves exact code/name matches and rejects conflicting meanings without overwriting. Numeric IDs are resolved dynamically.
5. Run backfill_owner_memberships with the validated owner-role ID. Insert missing valid active owners, preserve exact matches, fail on conflicts; never repair silently or alter staff.
6. Verify tables, columns, owned sequences, constraints, indexes, all six code/name pairs and complete owner coverage before COMMIT. Roll back on any error. Preserve existing business records; production counts need not be zero.
7. Do not call upgrade/downgrade, flask db upgrade, historical replay, stamping or db.create_all. Do not change Alembic. Migration source files are definitions/helper references only.
8. Release only whitelisted runtime files and reviewed integration hunks. Test real platform authentication/root and Retirement flow after the authorized release; clean only newly created test data.

Operator references: migrations/versions/retire_01_organisation.py, migrations/versions/retire_02_membership.py, docs/retirement_stage2_schema.sql and this manifest. Local-only tooling scripts are not production connection scripts.

## Remaining gates / parked debt

There is no remaining local Retirement workflow blocker. Production database/schema authorization, target preflight and actual deployed-baseline review remain required. No production connection or deployment occurred.

PARKED STARTUP SAFETY is recorded in AGENT.md. Unrelated Mech/CRM/SPV maintenance outside the skip guard remains unchanged. Isolation allowed local Retirement tests to proceed. Do not broaden this into cleanup. If an existing startup mutation demonstrably blocks a safe production restart, stop at that concrete issue and obtain authorization for the smallest safe isolation; do not assume SKIP_AUTO_MIGRATE suppresses all maintenance.

Task file changes: AGENT.md; scripts/retirement_stage2_local.py; tests/retire/check_stage2_local.py; docs/retirement_deployment.md; docs/retirement_integration.patch; docs/retirement_stage2_schema.sql; artifacts/retirement-stage2-deployment.zip. No runtime application source, UIP/core/other program or existing user record was edited. No Git staging, commit, reset or cleanup was performed. Existing dirty work was preserved.
