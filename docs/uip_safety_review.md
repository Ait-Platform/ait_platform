# Current-contract test cleanup ? completed locally

This section supersedes the earlier test totals below; the original safety-stage
handoff remains preserved. No commit, push, deployment or production database
action occurred. The complete safety repair remains local and uncommitted.

## Before / after

| Check | Before | After |
|---|---|---|
| Full isolated UIP suite | 307 passed / 76 failed / 17 errors | **378 passed / 24 failed / 0 errors** |
| Original safety/migration set | 69 passed | **69 passed** |
| Startup preservation harness | 1 passed / 4 errors | **5 passed** |
| Public-mandates regression | AttributeError observed | **1 passed** |

The full suite now contains 402 cases (two new tests). No failure was hidden with
skip/xfail, and the two deferred model/migration discrepancies still fail visibly.
All tests used the isolated PostgreSQL cluster/database from the safety stage;
neither the normal local application database nor production was accessed.

## Authoritative register fixture changes

The shared make_member/make_property helpers now call the actual production
process_import_batch service, with synthetic municipal source/batch identifiers,
received/effective dates and the authenticated fixture actor. They assert a
successful import and retrieve its resulting register record. They do not pass
is_import=True directly to low-level save functions or bypass authorization.

Cross-organization audit/intake/import fixtures use the same service. Eligible
voter and ownership fixtures import relationships by municipal member/property
references. Chronological ownership replacement tests verify preserved history,
sealed manual edits, and repeated-current-owner idempotence. Imported audit tests
now expect the existing import_id provenance alongside allowlisted changed_fields.
Foreign-account import rejection is tested as a recorded batch exception, rather
than expecting the importer to re-raise a per-row NotFound exception.

Intentional invalid-row inserts used only to test database FK rejection were
retained; they are not fixtures establishing legitimate authority. No production
register-service implementation or authority rule was changed in this stage.

The HTTP CSV endpoint is still tested separately and remains broken: it constructs
UipDocument without required filename. Service-fixture success is NOT evidence
that browser CSV upload works. Its exception response also exposes raw database
error details; this pre-existing gap was newly identified and left explicitly
unresolved rather than claiming the earlier log-privacy repair covers all responses.

## Removed routes / current navigation evidence

/members/new and /properties/new no longer exist. Current list templates point to
/register/import; existing-record view/edit routes remain. Updated register tests
verify imported records, sealed-field denial, permitted contact updates, import
permissions and 404s for removed creation URLs. CSRF/audit rollback tests now use
existing mutation routes instead of nonexistent creation routes.

Not all old links are genuinely dead: setup.html still builds member_form and
property_form URLs without mandatory IDs; older end-to-end/intake tests also
assume creation links. Those broader journeys remain unresolved. No creation
route was restored, and no UI/business flow was redesigned to mask broken links.

routes.dashboard explicitly redirects manager/receptionist and unrecognized-role
probes to my-access. The latter page presents access status rather than the old
six-card executive dashboard. Phase1, audit denial, work-order and page-export
expectations were aligned with that dispatch; executive service checks remain.

Current base.html places logout in the top bar; navigation.html adds a Personal
section and uses colored class names. The isolated logout test now supplies the
missing verify_ratepayer URL stub and checks the actual top-bar location, desktop/
mobile visibility, session clearing and remember-cookie removal. No authentication
or navigation production code was changed.

A remaining stale finance graph-head assertion was updated to the verified current
head uip_p52_billing_data. Its scoped migration-preservation assertions remain.
No migration, model, constraint or synthetic schema snapshot was changed.

## Startup and public-mandates repairs

The AST startup harness now provides an explicit os.getenv stand-in with
SKIP_AUTO_MIGRATE=1. Other keys return their defaults. This exercises the current
startup block using its existing in-memory session, while preventing migration
execution. All five preservation checks pass. Production startup was not modified
in this stage; its earlier safety-stage create_all removal is preserved.

Public mandates already orders adopted, organization-scoped resolutions by
newest decision_date (nulls last). Its nonexistent updated_at fallback is now
created_at, with descending ID as a stable final tie-breaker. This is the ONLY
production behavior change in this cleanup stage. A regression renders the actual
page anonymously, verifies adoption filtering and ordering, and observes no writes.
No fake timestamp column was introduced.

## Exact tests run and final results

All pytest commands use `python -B -m pytest --confcutdir=tests/uip` and
`-q -p no:cacheprovider --tb=short`. Runs requiring temp files also use a unique
writable artifacts/uip-contract-UUID --basetemp. UIP_TEST_DATABASE_URL points only
to localhost port 55439 / uip_test_safety. Existing disposable migration fixtures
were run unchanged; no Alembic CLI upgrade/stamp or schema repair was performed.

| Test selection | Result |
|---|---|
| test_register.py test_audit.py test_csv_options.py test_pilot.py | 56 passed, 11 failed, 0 errors |
| test_phase1.py test_work_order_access.py test_redesign.py | 29 passed |
| test_public_mandates.py | 1 passed |
| python -B tests/test_startup_enrollment_preservation.py | 5 passed |
| Original 69-test selection below | 69 passed |
| tests/uip (full suite) | 378 passed, 24 failed, 0 errors; 64.83 seconds |
| Additional logout / finance-page-export / sample-page-export check | 3 passed |

Original safety selection: test_safety.py, test_provisioning_final.py,
test_log_privacy.py, test_bridge.py, test_phase3_migration.py,
test_phase49_migration.py. All remain unchanged in this cleanup stage.

git diff --check passed. Full output is in artifacts/uip-contract-final-suite.txt;
focused outputs use artifacts/uip-contract-*-final.txt. Warnings remain mostly
existing datetime/SQLAlchemy deprecations. The disposable server is stopped after
reporting; its files are retained, not mixed with the normal development database.

## Remaining 24 failures ? classification

Counts below classify the currently observed primary blockers, not a promise that
fixing one will expose no later failure. No remaining setup errors exist.

| Class | Count | Cases / reason |
|---|---:|---|
| Stale journey/navigation contract requiring a business decision | 9 | Five visible_navigation_matches_permissions roles; intake_registration_roundtrip_and_empty_states; manager_full_visible_operational_journey; finance sidebar_active_group_and_palette; pilot templates_and_visuals. Old uniform sidebar/role/access expectations conflict with current access-status and appointment-specific routing. Some old creation links also remain in application templates and need a separately reviewed journey repair. |
| Genuine application defect | 12 | Eight CSV-options cases plus completion CSV preview/atomicity and import-scope cases are blocked by missing UipDocument.filename. Representative creation passes unsupported record_source to UipMemberRepresentative. Historical relationship imports can close a future-dated relationship before its start, violate ck_uip_property_member_dates, then continue using a failed transaction. |
| Known model/migration discrepancies ? deliberately untouched | 2 | test_revision_parent_and_model_schema_parity; test_phase11_parity_and_preservation. Historical single-column resolution meeting FK absent from current model, and uip_survey.created_by nullable disagreement remain visible. |
| Test-platform/schema-fixture dependency | 1 | test_concurrent_survey_response_records_one_eligible_vote uses the older phase49 concurrency fixture without the later eligibility_snapshot survey column. Left untouched because this stage must not alter the disposable schema or migration application to hide schema gaps. |

Shared production startup/VisitLog issues remain parked, independently of these
24 failures: generic auto-migration, unrelated startup DDL, and GET analytics
writes/commits/rollbacks were NOT repaired. Passing the AST harness is not a
certification that the complete shared production factory is side-effect-free.

## Exact files changed in THIS cleanup stage

1. app/program_uip/routes.py ? public-mandates ordering only, layered on prior safety edits.
2. tests/test_startup_enrollment_preservation.py ? isolated getenv dependency.
3. tests/uip/test_register.py ? authoritative import helpers/current sealed contracts.
4. tests/uip/test_audit.py ? import provenance, current route rollback/dispatch.
5. tests/uip/test_completion.py ? import metadata/foreign fixtures/ownership history.
6. tests/uip/test_intake_search.py ? authoritative imported register/relationship fixtures.
7. tests/uip/test_phase49.py ? imported eligible-voter ownership fixture.
8. tests/uip/test_phase1.py ? current my-access dispatch expectations.
9. tests/uip/test_work_order_access.py ? current dispatch expectations.
10. tests/uip/test_redesign.py ? import fixtures/access-status exports.
11. tests/uip/test_finance.py ? current graph head/access-status export only.
12. tests/uip/test_sign_out.py ? current navigation stub/top-bar logout checks.
13. tests/uip/test_public_mandates.py ? new query/render/read-only regression.
14. docs/uip_safety_review.md ? this dated cleanup handoff, preserving prior history.

No SACE, Reading, Retirement, payment/wallet implementation, production User,
migration files, schema snapshot, shared startup or VisitLog changes in this stage.
The pre-existing CONCLUDED provisioning status, SACE about.html, public welcome.html,
.gitignore and artifact/submodule work remain separate and untouched.

STOPPED for review. No commit hash: nothing was committed, pushed or deployed.

---

# UIP safety-stage handoff ? 2026-09-24

Status: local repair only. NOT committed, pushed, deployed, or production-ready.
The user conditioned commit/push on passing appropriate tests. The complete UIP
suite remains red, so that condition was not met. No next UIP development stage
has been started. No production or normal local application database was used.

## Findings verified against current code

- Duplicate User was a test bootstrap collision: root tests import the real model,
  while tests/uip/bootstrap.py defines a minimal User using the same db metadata.
  There is exactly one production user table declaration, app/models/auth.py.
  That file is unchanged. No extend_existing workaround was added.
- The separate-process UIP harness and confcutdir instructions already existed.
  Root collection did not exclude it, and current dependencies had outgrown its
  synthetic baseline. AuthSubject was missing; several governance/import tables
  and model columns were absent. BeautifulSoup was imported but not used by the
  obsolete provisioning tests; that dependency was removed rather than installed.
- _require_role returned None on denied mandatory checks. Callers that ignored
  the return value could continue. Mandatory checks now abort 403; optional
  routing probes retain their explicit None behavior.
- The existing admin invitation issuer signs org_slug/email with salt
  uip-provisioning. The receiving provisioning route ignored the token entirely
  and lacked login_required. It now requires login, verifies the signature and
  a seven-day expiry, validates organization and recipient, and carries the token
  through the existing form. The alternate Genesis Secretary POST also validates
  it before creating any appointment. Existing issuer/governance design was not
  redesigned. Committee activation already validated its separate seven-day
  signed token; that mechanism was not replaced.
- Anonymous GET reset-genesis/remove-trigger/apply-patch endpoints performed
  destructive writes or DDL. nuke-test-votes affected all organizations, and
  fix-meeting/revert-meeting changed historical dates on GET. These temporary
  HTTP maintenance bodies now return 410 (existing login guards remain where
  present). No replacement online maintenance mechanism was introduced.
- auto_patch_db was a before_app_request hook, so UIP DDL and commit/rollback
  could run even during requests to unrelated programs. It was removed.
- Entry GET created trial entitlements and soft-deleted duplicate organizations.
  Router/waiting-room/dashboard/finance GET logic repaired memberships, roles or
  positions; router/Secretary organogram seeded seats; campaign GET created rows.
  Those incidental writes were removed. Explicit existing Secretary seat-edit
  POST operations remain. Missing seats/data now require explicit authorized
  maintenance rather than browser navigation repairs.
- Startup still contained a broad create_all block explicitly added to bootstrap
  UIP Governance. Only that block was removed from shared app startup.
- UIP log privacy filtering already existed, but appended raw exception text.
  That append was removed. The 403 handler no longer writes stack/description
  files. Photo upload and register-import exceptions now use generic messages.
- The organization lookup hook itself already only queried existing records;
  it did not need a new provisioning or membership architecture. Existing
  CSRF checks and service authorization mechanisms were retained.

## Harness repair and migration evidence

Root collection ignores tests/uip; run it explicitly in a separate interpreter
with --confcutdir=tests/uip. Its database guard now refuses ait_local_db as well
as remote/SQLite URLs, accepting only explicitly local uip_test_* databases.

A frozen SQL fixture supplies current request-model dependencies to disposable
schemas, with explicit foreign keys/indexes. It is NOT a production migration.
The existing historical migration tests still run their specific scoped revisions;
no upgrade head, stamp, old sync migration replay, or db.create_all was used.
Concurrency fixtures use the same current dependency snapshot.

The actual graph now has one head, uip_p52_billing_data. Merge d3f70f6a0794 has
parents 7da57fffdba9, b3d537f90c43 and uip_p11_completion. Two old tests still
expected separate phase49/legacy heads; these assertions were corrected only
after reading that graph. No migration file was changed.

The pre-existing uncommitted provisioning status change SCHEDULED -> CONCLUDED
remains in the working tree; this safety stage did not introduce it. A pre-existing
uncommitted revert-meeting endpoint is now disabled as part of the maintenance
safety repair. Other unrelated working-tree changes were left alone, including
SACE about.html, public welcome.html, .gitignore, and artifact/submodule changes.

## Tests and exact commands

Tests used a newly initialized PostgreSQL 17 cluster bound to 127.0.0.1:55439,
database uip_test_safety. Its connection settings were provided only to the test
process. No .env change, production connection, or real/shared database access.
The cluster is stopped after validation; its disposable files are retained.

Focused command:

    python -B -m pytest --confcutdir=tests/uip tests/uip/test_safety.py tests/uip/test_provisioning_final.py tests/uip/test_log_privacy.py tests/uip/test_bridge.py tests/uip/test_phase3_migration.py tests/uip/test_phase49_migration.py -q -p no:cacheprovider --tb=short

Result: 69 passed, 544 warnings, 7.66 seconds.
Covers forged/missing/expired/mismatched/malformed invitations; anonymous denial;
authenticated founding appointment/replay; alternate Secretary-claim bypass;
mandatory role denial, inactive membership and legitimate role access; retired
maintenance denial; no DDL/repair writes on representative entry, router,
Secretary, finance and other navigation GETs; log privacy; single production
User declaration; startup create_all removal; migration preservation checks.

Full UIP command (TEST_TEMP was a unique writable artifacts/uip-pytest-UUID path):

    python -B -m pytest --confcutdir=tests/uip tests/uip -q -p no:cacheprovider --basetemp=TEST_TEMP --tb=short

Result: 307 passed, 76 failed, 17 errors, 7385 warnings, 63.18 seconds.
All 400 cases collected: no duplicate-user collision or missing bs4 dependency.
Warnings are mainly existing datetime/SQLAlchemy deprecations.

Additional command:

    python -B tests/test_startup_enrollment_preservation.py

Result: 1 passed, 4 errors. The unchanged AST-based startup test does not supply
os in its exec environment; current startup references os.getenv. This is a
harness failure, not evidence that production startup succeeds or fails. The
isolated UIP Flask application starts successfully in the focused tests. Full
shared application startup is NOT certified by this stage.

git diff --check passed. Existing real User and migration files are unchanged.
Logs: artifacts/uip-focused-final.txt and artifacts/uip-safety-final-suite.txt.

## Remaining failures and rollout blockers

Do not weaken business authorization merely to make historical tests green.
The full suite mixes multiple generations of UIP behavior; not every failure
has been individually resolved or proven pre-existing by an A/B baseline run.
Concrete observed categories:

- Old register/audit/CSV/pilot fixtures call manual authoritative ratepayer
  creation. Current register services correctly reject it; many failures and all
  17 remaining setup errors originate here. These require a separately reviewed
  authoritative-import fixture conversion, not reopening manual authority.
- Tests target removed register routes (404) and expect older manager/reception
  dashboards, while current dispatch redirects to my-access (302). Deciding
  which historical contracts remain valid is business-flow work beyond this stage.
- Model/migration parity is genuinely unresolved: the historical migration keeps
  a uip_resolution meeting FK that the current model omits, and uip_survey.created_by
  is NOT NULL in the migration but nullable in the current model. The fixture did
  not drop that FK or relax that nullability to disguise the discrepancy.
- Direct inspection/test of public-mandates raises AttributeError because it uses
  UipResolution.updated_at, which does not exist. Left unchanged as unrelated
  application behavior. The representative read-only safety checks use working
  navigation endpoints rather than claiming this route is healthy.
- Other failing areas include older presentation/sign-out contracts and governance,
  completion and finance expectations; the full failure log preserves each case.
- Shared startup still automatically invokes migrations unless SKIP_AUTO_MIGRATE=1,
  and unrelated startup DDL/maintenance exists outside that guard. Shared VisitLog
  request analytics still writes/commits/rolls back during GET requests. These were
  not rewritten. Consequently, a real-factory GET is not generally database-read-only.
- Purpose-specific existing UIP verification/claim actions can still persist
  relationship/claim records; this stage removed incidental repair/seeding, not the
  program's authority/claim business design.
- The merged migration graph and incomplete real local schema need a controlled
  schema reconciliation/restore rehearsal before any UIP deployment. Passing
  synthetic request tests is not evidence that production has the needed schema.

Recommended next review: approve the safety diff; agree the current authoritative
register/routing contracts; update those fixtures without weakening authority;
resolve the model/migration discrepancies in a separate migration plan; then
address shared startup isolation and rerun all regressions. No production rollout
or further implementation is authorized by this handoff itself.

## Exact files changed by this stage

- app/__init__.py
- app/program_uip/__init__.py
- app/program_uip/finance_routes.py
- app/program_uip/log_privacy.py
- app/program_uip/provisioning_routes.py
- app/program_uip/routes.py
- app/program_uip/secretary_routes.py
- app/program_uip/services/register.py
- templates/program_uip/provisioning.html
- tests/conftest.py
- tests/uip/README.md
- tests/uip/bootstrap.py
- tests/uip/conftest.py
- tests/uip/fixtures/current_request_schema.sql (new, test-only)
- tests/uip/test_bridge.py
- tests/uip/test_log_privacy.py
- tests/uip/test_phase49_migration.py
- tests/uip/test_provisioning_final.py
- tests/uip/test_safety.py (new)
- tests/uip/test_work_order_concurrency.py
- docs/uip_safety_review.md (this report)

No commit hash: no commit or push was performed. Render was not accessed or deployed.
