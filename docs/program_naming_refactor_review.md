# UIP and Retirement naming refactor review

Status: local changes only; STOPPED before commit, staging or push. No production database access, schema changes, Alembic execution or application-factory startup occurred in this task.

## Scope and baseline

This refactor preserves the existing dirty working tree, including changes that predate this task. The working tree is based on local HEAD 049c325; the previous production release c2d2388 was prepared in a separate checkout. Do not use the raw working-tree diff as a selective deployment package.

AGENT.md now records subject_ for legacy subjects, program_ for programs, program_<slug> for new standalone programs, and unchanged public/business/database identities. It also records Paystack as the sole target portal, prohibits introducing/extending other providers, and requires separately tested removal of legacy providers after dependency checks. No payment implementation or catalogue data was changed.

## Directory renames

| Before | After | Non-cache files |
|---|---|---:|
| app/uip | app/program_uip | 27 |
| app/retire | app/program_retire | 2 |
| templates/uip | templates/program_uip | 68 |
| templates/retire | templates/program_retire | 9 |

Updated Python package imports (including the factory's two imports), static/dynamic render_template paths, Jinja inheritance/includes, tests, UIP validation scripts and relevant documentation. Blueprint identities, endpoint identities, URL rules/methods, model imports under app.models, table names, catalogue slugs and business logic are unchanged. Exact original-to-result transformations were checked against pre-edit snapshots.

## Tests and registration

- Retirement source regression: 29 passed.
- Retirement local PostgreSQL workflow/security regression: 40 passed against explicitly verified ait_local_db. Cleanup removed only the 4 memberships and 2 organisations created by that test. Final local counts: organisations 0, memberships 0, roles 6.
- UIP menu/navigation regression: passed (secretary workspace, menu switch, committee redirect and waiting-lounge navigation).
- UIP database-free pytest selection: 8 passed, 2 failed. Both failures were reproduced against the captured pre-refactor source: test_log_privacy exposes synthetic exception contents; test_sign_out's isolated fixture lacks uip_bp.verify_ratepayer. These are existing issues and were not repaired in a naming-only task.
- 359 UIP tests were deselected because their fixtures require database/migration setup or they specifically test migrations/concurrency. No Alembic operations were run. test_provisioning_final.py could not collect because bs4 is absent; that database-dependent file was excluded on the subsequent run.
- Isolated actual factory import/registration statements: both blueprints register; all 111 route/endpoint/method tuples exactly match the pre-edit snapshot (UIP 100, Retirement 11). Prefixes remain /uip and /retire. Database connections were explicitly blocked for this check.
- All 75 HTML templates pass syntax and referenced-template checks, including inheritance/includes and literal Python render targets.

## Remaining old paths

No stale old package/template paths remain in active app/, templates/, tests/ or scripts/ source. The complete original dependency inventory is in program_naming_inventory.md. The final stale-reference list is artifacts/program-path-refactor/stale-references.json.

Historical root patch scripts, scratch experiments/logs and the immutable retirement_integration.patch retain old references deliberately; they are not runtime dependencies. The old release ZIP and artifacts/retirement-release checkout remain frozen historical artifacts. The updated deployment manifest warns not to apply these old artifacts to the renamed tree. Before/after audit snapshots, this review and the dependency inventory necessarily name the old paths.

Six inaccessible directories limited the repository-wide scan: templates/program_adv_math, .pytest_cache, three scratch/uip_ux_check_* directories and one scratch/uip_validation_* directory. None is one of the four renamed directories. Exact errors are preserved in artifacts/program-path-refactor/final-scan-errors.txt.

## Exact diff evidence

- artifacts/program-path-refactor/task-only.patch: git-generated, rename-aware diff between captured pre-task files and the final files, including new review/check documents. Excludes unrelated pre-existing edits.
- artifacts/program-path-refactor/task-diff-stat.txt: exact task-only git diff summary.
- artifacts/program-path-refactor/worktree-diff-stat.txt: raw tracked working-tree diff summary, which also includes unrelated pre-existing changes and omits untracked additions.
- artifacts/program-path-refactor/git-status-before.txt and git-status-after.txt: full status boundaries.

No commit or push was made. Full UIP regression is not green; the two verified baseline failures and unavailable migration-dependent coverage remain explicitly reported for review.
