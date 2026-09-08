# UIP interface redesign — local handoff

Implemented locally on 8 September 2026. No application deployment, production connection, production record creation or database migration was performed.

## Navigation

The UIP now has its own consistent application shell with a persistent 238px desktop sidebar, active-page indication and a collapsible mobile menu. Links follow the existing role permissions; provider-only users retain the restricted work-order interface.

| Section | Destinations |
| --- | --- |
| Command Centre | Overview |
| Residents & Properties | Ratepayers, Properties, Import Register |
| Operations | Interactions & Issues, Tasks / Follow-ups, Municipal Matters, Communications |
| Service Providers | Providers, Work Orders, Routing & SLA |
| Governance | Meetings, Surveys, Decisions, Documents |
| Reports | Reports / Exports |
| Administration | Organisation Settings, UIP Audit |

## Command Centre

Four primary actions lead to interaction capture, ratepayer registration, property creation and issue selection for a work order. Six real summary cards show open issues, outstanding tasks/follow-ups, active work orders, SLA breaches, open municipal matters and upcoming meetings.

Attention Required combines actionable SLA, work verification, overdue tasks/follow-ups, municipal, governance and assignment items. Recent activity shows six plain-language UIP audit events. A simplified current-issues table and one service-performance section complete the dashboard. Empty organisations receive one compact setup panel linking to the dedicated getting-started guide. Disabled AI controls and the previous wall of counters have been removed.

Ratepayer and property registers have search/status filters and linked records. Profiles group contact details, current relationships, history, related issues and eligibility; optional AIT account linkage is secondary. Intake has Reporter, Property and Interaction sections, searchable selectors and linked-property priority. Issues have Open, Unassigned, In progress, Waiting, Resolved and All filters. Provider registers show service capabilities and active work. Operational forms are grouped into expandable actions; displayed dates and lifecycle statuses are readable.

## Files and routes

Application changes are confined to `app/uip/routes.py`, `completion_routes.py`, `operational_routes.py` and the new read-only `presentation.py`. Services, models and migrations are unchanged.

Existing route behaviour is presented through `/uip/<org_slug>/dashboard`, `/members`, `/properties`, `/interaction/new`, `/providers` and `/operations/reception`. Register filtering uses query parameters. The only new route is the read-only `/service-standards` navigation landing page, pointing to existing SLA, routing and performance pages. Existing form submissions and lifecycle services remain authoritative.

New templates/styles: `templates/uip/base.html`, `ui.css`, `issue_table.html`, `operations/issues.html`, `service_standards.html`.

Updated templates under `templates/uip/`:

- `navigation.html`, `setup.html`, `register_import.html`, `register_pagination.html`, `validation_error.html`.
- `dashboards/manager.html`, `receptionist.html`, `committee.html`, `resident.html`, `reports.html`.
- `members/form.html`, `list.html`, `view.html`; `properties/form.html`, `list.html`, `view.html`.
- `providers/form.html`, `list.html`, `view.html`; `work_orders/list.html`, `view.html`, `provider_view.html`.
- `reception/new_interaction.html`, `interactions/view.html`, `operations/page.html`, `admin/settings.html`, `audit/list.html`.

Validation additions/updates: `tests/uip/test_redesign.py`, `test_completion.py`, `test_phase1.py`, `test_work_order_access.py`, `scripts/render_uip_ui.py`; the consolidated runner now writes a separate redesign log.

## Validation and inspection

The existing local PostgreSQL harness confirmed the loopback server (`::1`), successful connection and disposable-schema creation/drop. No production credentials or connection were used.

One consolidated UIP run executed all 304 tests: 301 passed; three failed because they explicitly expected the removed AI-unavailable text or individual dashboard dispatch counters. Those presentation expectations were updated while retaining lifecycle metric and permission assertions. A focused rerun of both affected modules passed all 23 tests, resolving all three failures. The full suite was not repeated.

All five startup-preservation checks passed. Python/Jinja parsing, unchanged migration ancestry and UIP product-isolation checks passed. Existing datetime deprecation warnings remain.

Local Chromium rendered all eleven requested pages at 1440×1000 and 390×844, generating 22 screenshots. Checks passed for one vertical desktop navigation column, one page title, no document-level horizontal overflow, collapsed mobile navigation, working menu toggle, ratepayer search and linked-property prioritisation. Desktop screenshots were visually reviewed, along with mobile Command Centre and intake. Corrections included heading hierarchy, control sizing, register subtitles and readable dates. Wide tables scroll within their cards on mobile.

Screenshots use synthetic records created only within the disposable test harness. Local artifacts are in `scratch/uip_ui_preview/`; `inspection.json` records measured layout results. The original consolidated output is retained in `scratch/uip_redesign_validation.txt`.

| Page | Desktop | Mobile |
| --- | --- | --- |
| Command Centre | [Image](../scratch/uip_ui_preview/command-centre-desktop.png) | [Image](../scratch/uip_ui_preview/command-centre-mobile.png) |
| Ratepayers | [Image](../scratch/uip_ui_preview/ratepayers-desktop.png) | [Image](../scratch/uip_ui_preview/ratepayers-mobile.png) |
| Ratepayer profile | [Image](../scratch/uip_ui_preview/ratepayer-detail-desktop.png) | [Image](../scratch/uip_ui_preview/ratepayer-detail-mobile.png) |
| Properties | [Image](../scratch/uip_ui_preview/properties-desktop.png) | [Image](../scratch/uip_ui_preview/properties-mobile.png) |
| Property profile | [Image](../scratch/uip_ui_preview/property-detail-desktop.png) | [Image](../scratch/uip_ui_preview/property-detail-mobile.png) |
| Log interaction | [Image](../scratch/uip_ui_preview/log-interaction-desktop.png) | [Image](../scratch/uip_ui_preview/log-interaction-mobile.png) |
| Issues | [Image](../scratch/uip_ui_preview/issues-desktop.png) | [Image](../scratch/uip_ui_preview/issues-mobile.png) |
| Providers | [Image](../scratch/uip_ui_preview/providers-desktop.png) | [Image](../scratch/uip_ui_preview/providers-mobile.png) |
| Work orders | [Image](../scratch/uip_ui_preview/work-orders-desktop.png) | [Image](../scratch/uip_ui_preview/work-orders-mobile.png) |
| Meetings | [Image](../scratch/uip_ui_preview/meetings-desktop.png) | [Image](../scratch/uip_ui_preview/meetings-mobile.png) |
| Surveys | [Image](../scratch/uip_ui_preview/surveys-desktop.png) | [Image](../scratch/uip_ui_preview/surveys-mobile.png) |

## Remaining usability limitations

Scheduling and follow-up forms still require explicit UTC offsets, preserving their existing input contract. A timezone-aware date/time picker would be a useful further refinement. Creating a work order still requires selecting an issue and then its eligible provider; it is not a standalone creation form. Detailed lifecycle histories retain full references and audit information for traceability.

SACE, Reading, HealthCore, SPV, Thunee, payments and shared unrelated screens were not modified. No changes were committed or pushed as part of this redesign.
