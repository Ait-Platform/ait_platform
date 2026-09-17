# Retirement production provisioning result

Production database: `ait_platform_db`.
Authenticated session: `aitplatformdb_retirement`.
Effective role: `ait_platform_db_user` (expected).

Stage 1 dependency checks passed: public user.id is a non-null integer primary key; schema usage/create and user.id references privileges are present; intended Retirement relation/type/constraint names were absent.

One controlled transaction created retirement_organisation using the approved definition, validated Stage 1, created retirement_role and retirement_membership with the prepared indexes and constraints, established all six canonical roles and performed the empty owner backfill. Full prepared schema validation passed before commit and again after commit. No migration entry point, historical migration chain, migration stamping, application factory or db.create_all was invoked.

Final verified production counts:

| Table | Rows |
| --- | ---: |
| retirement_organisation | 0 |
| retirement_membership | 0 |
| retirement_role | 6 |

All 29 database-free Retirement regression checks passed. Prepared package source files match the workspace. Existing unrelated dirty files were not changed.

Deployment and live workflow/security testing remain blocked: no connected browser, Render CLI or Render API credential was available to inspect the service, establish its deployed baseline, verify SKIP_AUTO_MIGRATE=1 and trigger/verify the selective release. No code was pushed or deployed. No production workflow test records were created, so no cleanup was required. Local checks are not a substitute for live workflow validation.

Resume by obtaining authenticated Render access, reviewing the actual deployment baseline and skip setting, releasing only the manifest runtime files and required selective integration hunks, then executing the authorized live Retirement workflow/security tests and cleaning only their Retirement test records.
