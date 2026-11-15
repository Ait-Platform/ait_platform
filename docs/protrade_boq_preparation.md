# BOQ preparation time

BOQ creation, saved-quotation item editing and section preparation now send private activity check-ins. Tracking applies to existing authorised BOQ users, including Happiness, without hard-coding a particular account. No timer, totals or timing controls appear in their workflow. Admin Hub → BOQ Preparation Time opens the paginated, Admin-only report at `/mechanic/admin/boq-preparation`.

An unsaved preparation uses the quotation form's UUID, retained in browser storage scoped by user, business and debtor. The first save attaches that preparation to the existing quotation ID and database-generated quotation number. This does not create empty quotations or allocate numbers on GET. Clearing browser storage before saving can abandon an unsaved preparation; already-saved quotations retain their database identity and accumulated time.

While the quotation workspace is visible and focused, check-ins occur every 15 seconds. Keyboard, pointer and scroll activity reset the 60-second idle limit. Hiding the workspace, switching tabs, leaving the page or losing focus pauses tracking; returning resumes it. The server uses UTC timestamps, row locks, unique user/quotation constraints, per-tab leases and ordered request counters. It accepts continuous intervals of at most 20 seconds, and excludes longer unconfirmed gaps rather than counting offline hours. A lost final check-in can undercount the final interval. This is activity-based working-time measurement, not proof of continuous attention.

The total freezes when a complete BOQ PDF is successfully generated (all current sections must already be paid), when a complete BOQ email is accepted by the existing transport, or when the existing New Quote workflow retires the draft. Saving a draft, generating one section, previewing, failed output and downloading an individual section do not complete it. Reopening or correcting a completed quotation does not restart preparation time.

The new `protrade_boq_preparation` table is independent of debtor contacts, Work Activity Register, ledger and token receipts. No existing records are backfilled or rewritten. Dates in the Admin report are explicitly UTC. The report includes unsaved preparations and provides pagination for older records. An authenticated, account-scoped, non-cached GET refreshes the CSRF token during long sessions; all timing writes remain protected POST requests.

## Deployment

A database migration **is required**: `protrade_boq_time_016`, following `protrade_work_activity_015`. The additive chain is `012 → 014 → 013 → 015 → 016`. Apply 016 before making the new BOQ workflow available. The old application remains compatible with this additive table. Do not stamp past migrations, recreate numbering triggers, or use `create_all()` as a substitute.

Use the authenticated Render Shell, deployed release files and the running service's approved PostgreSQL configuration. Independently verify the target database/user before any production write, following AGENT.md. Do not copy credentials into the repository or local `.env`. From the repository root, the following programmatic commands override the historical Alembic INI script-location error and load model metadata without application startup maintenance:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.current(c)"
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_boq_time_016')"
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.current(c)"
```

Verify revision 016, restart the service, and check BOQ preparation and the Admin report. Production migrations were **not executed** during development. Downgrade intentionally refuses to erase preparation history; roll back application code while retaining the additive table if needed.

## Verification

Local PostgreSQL tests use isolated temporary tables and the actual production model/routes. They cover pause/resume, interrupted sessions, duplicate/out-of-order requests, concurrent-tab leases, user/business isolation, first-save attachment, finalisation, denied non-Admin reporting, unchanged balances/ledger and migration uniqueness. The full Alembic 012 → 016 rehearsal preserves all original financial/quotation rows and both ledger function definitions. Headless Chromium tests exercise the actual browser script, inactivity, workspace/tab visibility and unsaved refresh persistence. The maintained regression selection excludes the documented stale `ProTradeDashboardTests` fixture.

Verified on 15 November 2026: `python -m pytest tests -q -k "protrade and not ProTradeDashboardTests" --tb=short --disable-warnings` passed **220 tests and 37 subtests**. The focused timing, browser and quotation-fixture selection passed 19 tests. Unrelated working-tree files were preserved.
