UIP operational completion pass — 8 September 2026

Implemented locally against the existing Phase 1–9 architecture. No production connection, migration, application deployment or Git commit/push was performed during this completion pass.

1. Missing journeys found: intake prioritised login residents over registered ratepayers; new registrations did not return to intake; operational tools were scattered across dashboards; empty organisations lacked setup guidance; related-record return links, readable validation, document metadata editing, meeting editing/cancellation and SLA deactivation were incomplete. The obsolete resident intake link led to a forbidden route and placeholder quick links were removed.
2. Added routes/forms/templates: role-aware shared navigation; `/getting-started`; `/operations/tasks`, `/operations/municipal`, `/operations/communications`, `/operations/routing`; `/register/import`; CSV preview/confirmation; readable validation; document metadata edit; scheduled meeting edit/cancel; SLA deactivation; decision follow-up task creation. All paths are under `/uip/<org_slug>` and use existing scoped services and UIP audit.
3. Members / Ratepayers: existing Phase 2 manual create/view/edit/active/inactive/contact/eligibility and communication preferences remain available. Non-login capture creates no User or role. Optional selection of an existing active organisation account shows account names. The schema supports `person` and `business`; company, trust, body corporate and other organisations use the existing business classification, with their full name. No new member system or type migration was introduced. Existing account associations remain protected against repurposing.
4. Properties and relationships: visible register, create/edit/detail and member-to-property navigation. Creating a property from a member retains the selected member. Existing dated ownership/occupancy/representation structures are reused. Overlapping duplicate relationships are rejected; original start dates and closed end dates cannot be rewritten. End a current relationship and add a new one to retain history. Related issue links are visible only to the existing authorised issue-reader roles.
5. CSV import added: members, properties, then relationships by organisation-scoped references. Exact CSV headers are displayed. Maximum 200 rows / 256 KB of UTF-8 per file. Preview runs the same services/audit inside a rolled-back savepoint. Confirmation requires the same actor, organisation, import type and file hash, signed with a 30-minute expiry; the entire file is revalidated and committed atomically. Duplicate references, possible duplicate member emails/property addresses, duplicate dated relationships and invalid rows are reported without overwriting. References from another organisation never match. Representatives/preferences remain manual. No generic import framework, account creation or outbound messaging.
6. Command Centre: role-aware navigation across UIP pages exposes the real implemented registers and workflows. Empty member/property registers prompt the manager to register the first ratepayer/property, use CSV preview or follow the setup guide. No fabricated data or completion percentages.
7. Lifecycle completion: intake selects active scoped members/properties, offers general enquiry, shows explicit empty states and returns after registration with selections. Free-text issue titles replace the restricted topic list, and saving opens the issue. Issue details link to reception/municipal/follow-up/communication actions. Provider association and missing-account requirements are visible; existing provider-only acceptance/progress/completion/rejection/failure and manager verification/closure/cancellation permissions remain intact. Terminal unsuccessful work orders link back for replacement. Documents preserve file versions when current metadata changes. Scheduled meetings can be edited/cancelled; started/concluded history remains protected. Meeting participant choices derive from dated eligibility, and frozen quorum counts are displayed. Surveys retain eligibility, privacy and duplicate-response guards, with links from results to decisions. Formal decisions retain correction/supersession instead of destructive editing.
8. Settings: visible links to quorum rules, category/priority/stage SLA configuration and deactivation, and provider category capabilities/availability/account associations used by routing. Existing clock policy snapshots remain intact. No area/capacity rules or delivery mechanisms were invented.
9. Exact changed/new files are listed below. Existing untracked `h` and `scratch/uip_prod_migration.py` are not part of this change.
10. Migration: none. No model or existing migration changed. UIP ancestry remains `uip_p2_prod_base -> a27c9e4b6102 -> uip_p3_work_orders -> uip_p49_operations`; the legacy branch is unchanged.
11. Validation: verified the existing loopback PostgreSQL `ait_local_db` connection with disposable-schema creation/drop. Targeted development checks were followed by one consolidated run: 297 passed, one reserved-route naming conflict found. Renamed guidance from `/setup` to `/getting-started` without weakening the original maintenance prohibition. The affected maintenance check plus all 13 new completion cases then passed (14 passed). All 298 distinct UIP tests have passing results; the entire suite was not rerun. Five startup-preservation tests passed. Parsing passed for 38 Python files / 31 UIP templates; existing schema-parity, migration and isolation checks passed. Full evidence: `scratch/uip_completion_validation.txt`. The 13-step practical scenario was exercised through Flask's test client, actual rendered links/forms, CSRF and services in local disposable PostgreSQL schemas; provider/resident actions used the proper roles and only test time was advanced to finalize a poll. This was not a live-production browser test or visual screenshot review.
12. Remaining work: no identified implementation blocker in the requested existing-schema journeys. Production deployment remains deliberately unperformed. Real provider/member accounts must already be authorised through existing account administration; registering a provider or ratepayer never grants application roles. Non-login ratepayers can be captured and represented, while online survey responses still require the member's or verified representative's linked account. External invitations/dispatch/communications must actually occur outside the log before operators record them as such.

Exact application, template, validation and report files:

- UIP_OPERATIONAL_COMPLETION.md
- app/uip/__init__.py
- app/uip/completion_routes.py
- app/uip/operational_routes.py
- app/uip/routes.py
- app/uip/services/audit.py
- app/uip/services/documents.py
- app/uip/services/governance.py
- app/uip/services/register.py
- app/uip/services/sla.py
- scratch/uip_completion_validation.txt
- scripts/validate_uip_phase49.py
- templates/uip/admin/settings.html
- templates/uip/audit/list.html
- templates/uip/dashboards/committee.html
- templates/uip/dashboards/manager.html
- templates/uip/dashboards/receptionist.html
- templates/uip/dashboards/reports.html
- templates/uip/dashboards/resident.html
- templates/uip/interactions/view.html
- templates/uip/members/form.html
- templates/uip/members/list.html
- templates/uip/members/view.html
- templates/uip/navigation.html
- templates/uip/operations/page.html
- templates/uip/properties/form.html
- templates/uip/properties/list.html
- templates/uip/properties/view.html
- templates/uip/providers/form.html
- templates/uip/providers/list.html
- templates/uip/providers/view.html
- templates/uip/reception/new_interaction.html
- templates/uip/register_import.html
- templates/uip/relationship_dates.html
- templates/uip/setup.html
- templates/uip/validation_error.html
- templates/uip/work_orders/list.html
- templates/uip/work_orders/provider_view.html
- templates/uip/work_orders/view.html
- tests/uip/conftest.py
- tests/uip/test_completion.py
