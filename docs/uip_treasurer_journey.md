# Treasurer Board and Financial Tools ? local implementation

No commit, push, deployment, production action, schema or migration change.
Earlier uncommitted UIP safety/RP work is preserved. MO, Chair/Vice Chair control,
Secretary redesign and new accounting functionality remain out of scope.

## Journey and existing components

- `/uip/<org>/treasurer-workspace`: existing 3x3 elected-member Board retained.
- Sidebar **Treasurer Board** returns there.
- Sidebar **Sec Control** opens the existing `/uip/<org>/secretary-workspace` directly,
  using the existing Secretary guard that already permits current Treasurers. It no
  longer sends the Treasurer through committee dispatch back to their own Board.
- Sidebar **Financial Tools** opens existing `/uip/<org>/finance`.
- Finance now uses the standard Phase 10 overview/template renderer for Treasurers
  too. Normal UIP navigation and the existing permission-controlled write buttons
  remain available. No second finance implementation was introduced.
- Existing transactions/new/detail, budgets, commitments/detail and six report
  routes are reused. The old `dashboards/treasurer.html` is retained on disk but is
  no longer selected by `finance_overview`; its hidden-sidebar/unconditional-write
  controls therefore no longer govern the Treasurer financial journey.

## Authority

A dedicated finance-local check looks up the active user and an existing
`UipCommitteeMember` in the requested organization, `status=CURRENT`,
`position=Treasurer`, with normalized email matching the authenticated account.
That appointment alone permits Phase 10 reads/writes: no extra core membership,
manager role or committee-role assignment is required or manufactured.

Existing manager authority is preserved through the original audit authorization.
Ordinary member finance reads retain their existing core-role/membership and
published-only transparency rules. Their writes remain denied. Shared/global
`audit.authorize` is unchanged. The legacy finance helper name `manager()` now
reports finance write capability (manager OR current Treasurer); it does not assign
or imply a manager role anywhere else.

Finance checks are applied both at HTTP entry and within read/write/audit services.
Organization entitlement, CSRF, organization row locking, Decimal validation,
version/request-key checks, immutable revisions, financial calculations and audit
actor attribution are unchanged.

The Treasurer workspace, voting-room, resolution-detail and vote POST routes now
require the current elected Treasurer appointment before reading or mutating their
working data. Being a manager, another committee member, merely authenticated or
entitled does not bypass these Treasurer-only checks.

## Existing associations and document boundary

Provider/work-order/resolution associations retain their scoped validation. The
finance document selector now queries documents in the organization and filters
through the existing per-document access check, rather than failing the entire
finance form at the generic document-list core-role guard.

Document classification/download authorization is not broadened: appointment-only
finance authority does not independently grant access to otherwise restricted
controlled documents. Only documents the actor can already access are selectable
or attachable; ordinary document ACLs still apply. No document upload, R2, RP photo,
Vault or shared storage change was made.

## Validation

Only disposable UIP PostgreSQL `uip_test_safety` at localhost port 55439 was used,
with isolated schemas and transaction rollback. The shared factory/normal local
and production databases were not used. Cluster stopped after validation.

- New Treasurer journey tests: **13 passed**.
- Existing finance tests: **32 passed, 1 failed**.
- Existing UIP safety regression: **69 passed**.
- Completed RP journey regression: **28 passed**.

The combined Treasurer + finance + safety run is **114 passed, 1 failed**.
The separate RP regression is **28 passed**.

The one failure is the already-documented
`tests/uip/test_finance.py::test_sidebar_active_group_and_palette` (line 223).
Its regex requires the exact class `ui-nav-group` and eight groups, whereas the
existing navigation uses additional color classes and changed groups. This same
failure is recorded in `docs/uip_safety_review.md`. It was not modified or repaired.
All other existing finance tests, including transaction/accounting acceptance,
manager/member permissions, privacy, concurrency and migration tests, passed.
Migration tests ran only inside disposable schemas; repository migration files
and application/production schema were not changed.

Focused tests prove appointment-only Board/finance access with no core role or
membership, direct Secretary access, sidebar links, denied former/foreign/other
appointments, Treasurer-only route protection, no navigation identity writes,
budget creation/revision, income/adjustments/expenditure, partial commitment
payments/revisions/cancellation, reversals/replacements, visibility and reports,
and ordinary member read-only UI and server-side denial. Financial audit events
are attributed to the Treasurer's user ID.

Outputs: `artifacts/uip-treasurer-tests.txt` and
`artifacts/uip-treasurer-rp-regression.txt`. No live browser or production test was
performed. Existing deprecation warnings remain.

## Exact files changed in this stage

1. `app/program_uip/services/finance.py` ? appointment recognition and finance-local authorization.
2. `app/program_uip/finance_routes.py` ? finance authorization, normal overview renderer, document-selector boundary.
3. `app/program_uip/committee_routes.py` ? four Treasurer-only route guards.
4. `app/program_uip/completion_routes.py` ? recognised Treasurer navigation context/finance links.
5. `templates/program_uip/navigation.html` ? Treasurer Board, Sec Control, Financial Tools links.
6. `tests/uip/test_treasurer_journey.py` ? focused PostgreSQL coverage.
7. `docs/uip_treasurer_journey.md` ? this report.

Some files also contain earlier uncommitted safety/RP changes. Those are preserved;
the full working-tree diff is broader than this stage. No new role table or migration
is required. Stop here.
