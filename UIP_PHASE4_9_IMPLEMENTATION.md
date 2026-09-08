**UIP Phase 4–9 local implementation handoff — 8 September 2026**

Implemented locally on the existing Phase 3 architecture. Nothing was deployed, and no production database was connected to or migrated. Database validation used the existing local `ait_local_db` connection and disposable `uip_test_<uuid>` schemas. Credentials were neither printed nor saved into source files or persistent environment settings.

**1. Features by phase**

| Phase | Implemented |
|---|---|
| 4 — Routing and SLA | Deterministic recommendations use organisation, issue category, active provider, availability, capability, authorised provider-user association and active workload. Staff still explicitly create and dispatch orders. Manager-configured SLA policies cover acknowledgement, dispatch, acceptance, commencement, completion and closure per organisation/category/priority. Each timer retains its policy and target. Status distinguishes within target, approaching breach, breached, met, completed late and stopped. |
| 5 — Reception and communications | Issue-linked reception actions, relevant member/property issue history, structured follow-ups/contact attempts, next actions and due dates, completion evidence, existing CoreTask creation, municipal referrals and communication logs. Municipal transitions preserve actor, date, reference, reason and version. Logging never sends a message or claims delivery. |
| 6 — Controlled documents | Organisation folders and categories, title, visibility, effective date, uploader, file type, size, SHA-256, replacement reason and version history. Files use private instance storage, sanitised original filenames and unique storage keys. Earlier versions remain downloadable only to authorised users. |
| 7 — Meetings and quorum | Meeting type, title, schedule, venue/online details, agenda, invitees, attendance, apologies and minutes. Quorum configuration specifies relationship, percentage and minimum. Eligibility comes from active eligible register profiles and verified dated property relationships; verified dated representatives can attend for a member. The actual meeting-start basis and concluded quorum are preserved. |
| 8 — Surveys and decisions | Dated surveys with controlled single-choice, yes/no and rating answers; eligibility captured with each response; one response per eligible member, including concurrent submissions. Confidential surveys expose final aggregates; identifiable responses are available to governance administrators only when explicitly configured. Decisions retain meeting vote/quorum or finalized survey basis, responsible actor, optional existing CoreTask and immutable correction/supersession history. |
| 9 — Command Centre and reports | Recorded issue, work-order, SLA, provider, municipal, follow-up, task and governance metrics; priority/category breakdowns; recorded timing means with sample counts; provider workload/completion/SLA sample counts; organisation-scoped CSV issue export with spreadsheet-formula protection. Committee and management reporting placeholders now use persisted records. |

**2. Exact models and tables**

| New model | Table |
|---|---|
| UipSlaPolicy | uip_sla_policy |
| UipSlaClock | uip_sla_clock |
| UipFollowUp | uip_follow_up |
| UipReferralEvent | uip_referral_event |
| UipCommunicationLog | uip_communication_log |
| UipDocumentFolder | uip_document_folder |
| UipDocumentVersion | uip_document_version |
| UipQuorumRule | uip_quorum_rule |
| UipMeetingParticipant | uip_meeting_participant |
| UipSurvey | uip_survey |
| UipSurveyResponse | uip_survey_response |
| UipDecisionEvent | uip_decision_event |

Four existing UIP tables are extended:

| Model/table | Added or changed fields |
|---|---|
| UipMunicipalReferral / uip_municipal_referral | `organization_id`, `version`; tenant-composite issue FK and identity constraint. Organisation is derived from the existing linked issue during migration. |
| UipDocument / uip_document | `title`, `category`, `folder_id`, `current_version`; tenant-composite folder FK and identity constraint. Historical metadata starts at controlled version zero; no file content is invented. |
| UipCommitteeMeeting / uip_committee_meeting | `agenda`, `eligibility_basis`, `quorum_rule`, `eligible_count`, `attendance_count`, `required_quorum`, `quorum_achieved`, `quorum_recorded_at`, `quorum_recorded_by`; tenant identity constraint. Historical quorum remains unknown. |
| UipResolution / uip_resolution | `organization_id`, `survey_id`, `decision_date`, `recorded_by`, `responsible_user_id`, `result_basis`, `supersedes_id`; `meeting_id` permits NULL for survey decisions. Exactly one source is required. Tenant-composite source/supersession FKs preserve organisation boundaries. |

CoreInteraction, CoreTask, UipProvider, UipWorkOrder and UipAuditEvent are reused. No new Core tables or Core schema changes were needed. New work-order journal actions explicitly capture their occurrence time; existing historical actions are unchanged. Important mutations and their UIP audit events share the caller's transaction.

**3. Migration revision chain**

`uip_p2_prod_base -> a27c9e4b6102 -> uip_p3_work_orders -> uip_p49_operations`

The new additive revision is `migrations/versions/uip_p49_operations.py`. Existing migrations are unchanged; legacy head `7da57fffdba9` remains separate. The revision contains frozen PostgreSQL DDL, not runtime application-model imports. History tables have database-level mutation protection; concluded meetings, attendance and survey designs/results are protected. Downgrade is deliberately refused to preserve history.

**4. Routes/pages added**

All routes are prefixed by `/uip/<org_slug>/operations`:

| Route | Purpose |
|---|---|
| `/sla` | SLA policies and recorded targets |
| `/reception` | Issue queue and outstanding action counts |
| `/reception/<issue_id>` | Follow-up, acknowledgement, contact, tasks and referrals |
| `/municipal/<referral_id>` | Municipal lifecycle/history |
| `/documents` | Controlled document register and folders |
| `/documents/<document_id>` | Replacement and version history |
| `/documents/<document_id>/versions/<version>/download` | Authorised private attachment download |
| `/meetings` | Meeting register and quorum configuration |
| `/meetings/<meeting_id>` | Attendance, meeting start, minutes and quorum capture |
| `/surveys` | Survey register and creation |
| `/surveys/<survey_id>` | Eligibility-checked response and final results |
| `/decisions` | Formal decisions, corrections and action status |
| `/providers` | Provider workload and recorded completion/SLA history |
| `/report.csv` | Organisation issue export |

Existing dashboards, management reports, intake, issue assignment, municipal escalation and work-order services are connected to these capabilities. Generic authentication routing was not modified.

**5. Role/permission model**

| Role | New capabilities |
|---|---|
| Manager | Operational administration, SLA policy configuration, governance administration, controlled documents and reports. |
| Receptionist | Routing/SLA visibility, acknowledgement, follow-up/contact logs, municipal operations, existing internal tasks and operational exports. Can read STAFF/MEMBERS documents; cannot upload documents or administer governance. |
| Committee member | Meetings, quorum, surveys, decisions and permitted controlled documents. Existing municipal escalation access is retained. |
| Owner / resident | Permitted organisation documents and survey participation pages. A ballot requires independently verified dated eligibility or representation; the account role alone never grants a vote. |
| Provider-only user | Existing explicitly associated provider-facing work orders only. All new operational/governance pages are denied. |

Document visibility is explicit: PRIVATE = manager; STAFF = manager/receptionist; COMMITTEE_ONLY = manager/committee/owner; MEMBERS = manager/receptionist/committee/owner/resident. All require active organisation account access. Survey administration does not confer participation eligibility.

**6. Command Centre now implemented in code**

The manager dashboard displays open/unassigned/resolved issues; work-order lifecycle counts; SLA stages within target, approaching, breached, met and completed late; acceptance delays; awaiting verification; provider availability; municipal lifecycle and recorded overdue dates; outstanding follow-ups, unresolved contact attempts and overdue tasks; upcoming meetings, quorum outcomes, open surveys and unresolved decisions. Priority/category counts and valid recorded timing means include their actual sample sizes. Provider reporting shows counts rather than unsupported performance rankings. The committee dashboard uses actual governance/referral counts.

**7. Tests created**

55 new cases across `tests/uip/test_phase49.py`, `test_phase49_migration.py` and `test_phase49_concurrency.py`. Coverage includes SLA boundaries and timestamps, configuration snapshots, routing eligibility/workload, follow-up validation, municipal history, communication non-delivery, private file versions, dated governance eligibility, frozen quorum, confidential surveys, duplicate voting, corrections, tenant/role/CSRF boundaries, CSV safety, rollback, history protection, migration preservation and concurrent writes. Existing fixtures were advanced to the new UIP revision; their disposable-schema safeguards remain in place.

**8. Tests actually run**

Targeted development runs: model/schema parity passed; 43 of the initial 44 feature cases passed, exposing a transaction-start timestamp issue; that issue was fixed and passed its focused rerun alongside migration and concurrency checks.

The single final consolidated run passed:

| Validation | Result |
|---|---|
| All UIP Phase 1–9 tests | 285 passed in 36.05 seconds |
| Startup-preservation tests | 5 passed |
| Python parsing | 36 files passed |
| UIP Jinja parsing | 27 templates passed |
| Model/schema parity | Passed in the full UIP suite |
| Migration ancestry and preservation | Passed |
| Product isolation/security checks | Passed |

The full suite was run once. A final preservation review then ensured municipal closure/reference updates retain the original recorded resolution timestamp and do not invent one for unresolved closures; the strengthened municipal lifecycle test passed in a focused rerun (1 passed). The recorded consolidated log is `scratch/uip_phase49_validation.txt`. Existing `datetime.utcnow()` deprecation warnings remain; no unrelated global datetime refactor was performed.

**9. Known limitations and policy choices**

SLA uses elapsed UTC time, with no business-hours calendar or inferred pauses. Intake starts acknowledgement/dispatch; dispatch starts acceptance; acceptance starts commencement; commencement starts completion; first completion starts verification/closure. Rework does not reset closure. Terminal unsuccessful orders stop outstanding order timers and retain any breach at stopping. Historical records are not backfilled with response times or quorum results.

Governance currently supports one eligible member unit per verified owner/occupier, even when that member has multiple properties. Quorum configuration is mandatory. Eligibility is captured at actual meeting start and at survey response; historical meetings are not retroactively certified. The survey page creates up to three questions; the service validates up to 30. Decisions can link one existing CoreTask; they do not introduce a separate action system.

Controlled uploads support PDF, PNG, JPEG and UTF-8 text, with a default 10 MiB limit. Legacy document metadata without a controlled stored file has no fabricated download. Reporting is a practical persisted-data view/export, not a BI system. No issue-reopening workflow or invented reopened counts were added.

The default application database schema was not migrated for interactive use. Migrations were exercised only in disposable test schemas. Local code is ready for the separately authorised restored-production rehearsal.

**10. Deliberately unavailable**

AI Auto-Triage/Luna and AI reports remain unavailable. Actual email/SMS/WhatsApp delivery is unavailable: communication records support delivery-unavailable status and future integration, but there is no active dispatcher or claim that logging sent a message. Unconfigured SLA targets, missing historical measurements and unavailable legacy file content remain explicitly absent.

**11. Product isolation and organisation portability**

No changes were made to SACE, HealthCore, SPV, Thunee, Reading, payments or CoreAuditEvent. UIP audit writes remain in UipAuditEvent. `site_hit` and `visit_log` were not touched. Pre-existing inaccessible `templates/program_adv_math` files were left alone.

UIP operational code uses organisation IDs and `org_slug`; it contains no Manor Gardens-specific implementation assumptions. The three pre-existing generic-auth redirects remain unchanged and are reported separately: `app/auth/routes.py:532`, `app/auth/routes.py:1359`, and `app/auth/routes.py:1428` still reference `manor-gardens`.

**12. Recommendation for one restored-production rehearsal**

After separate authorisation, restore the Phase 3 production backup into an isolated local PostgreSQL database, using local-only credentials and retaining the original backup. Confirm its revision is `uip_p3_work_orders`; capture existing row values and protected-product schema fingerprints before migration. Explicitly set `DATABASE_URL` to that restored local database for the rehearsal process only, then run from the repository root:

```powershell
python -B -m alembic -c alembic.ini upgrade uip_p49_operations
```

Use direct Alembic, not Flask startup, `upgrade head`, `stamp` or the legacy branch. Compare all pre-existing business columns and protected-product fingerprints afterward; confirm derived referral/decision organisation IDs and absence of invented historical clocks, quorum and file versions. Perform one role-based local smoke rehearsal covering provider assignment, configured SLA, referral history, document replacement/download isolation, dated quorum, duplicate/confidential survey response and decision correction. Actual delivery and AI remain disabled. Keep the rehearsal results and discard the restored clone afterward; do not downgrade history tables or promote the clone to production. Production migration/deployment remains a separate future authorisation.
