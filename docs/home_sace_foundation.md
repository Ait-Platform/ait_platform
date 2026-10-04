# Independent HOME SACE endorsement foundation

This document describes the original foundation. Its grant and completion
descriptions are superseded by [HOME Phase 2A](home_sace_phase2a.md).

HOME - Hands-On Math Education is a separate endorsement entity. This module has
no dependency on LITRE authority, codes, assignments, evidence or completion helpers.
Shared platform User identity and layout are reused. Ordinary HOME educational
content is read-only input; endorsement exercises must never write participant progress.

## Migration and persistence

Revision: `home_sace_001`; independent Alembic branch: `home_sace`; no parent.
The repository already has several unrelated migration heads. Apply only this revision
in an approved deployment procedure, never `upgrade heads` for this feature.
The migration contains only HOME table/index creation. Downgrade drops those tables
in reverse dependency order. It neither inserts nor alters existing programme rows.
No auth_subject, enrolment, platform admin or LITRE grant is created.

| Model | New table | Relationship / responsibility |
| --- | --- | --- |
| HomeController | sace_home_controller | Unique shared user identity; HOME-only active authority |
| HomeProvisioning | sace_home_provisioning | Hashed, email-bound entry token, issuing operator, expiry and claiming user/time |
| HomeInvitation | sace_home_invitation | HOME controller FK; hashed single-use code, status and expiry |
| HomePledge | sace_home_pledge | User FK; exactly one HOME provisioning or invitation FK; role, signature, text hash/version and timestamps |
| HomeAssignment | sace_home_assignment | Unique invitation FK; Auditor user FK; HOME status, requirements version and completion timestamp |
| HomeDocument | sace_home_document | Unique HOME material kind and title |
| HomeDocumentVersion | sace_home_document_version | Document FK, unique document/version, private storage key, PDF hash, source manifest and approving HOME controller |
| HomeEvidence | sace_home_evidence | Assignment FK, actor user FK, item/event, optional exact document-version FK and timestamp |

There is no shared completion record. HOME completion is stored on its assignment
and recorded in HOME evidence. The foundation cannot complete an examination while
the experience wrapper/materials are unavailable. Certificate applicability and final
HOME examination requirements remain to be confirmed when that content is integrated.

## Entry and authentication

The standard first-R entry is `/sace/home/provisioning`. It starts a HOME-only,
15-minute session nonce, then requires the HOME IP pledge and authentication.
No controller authority or database provisioning record is created on entry.
At successful claim, the nonce becomes a single-use `HomeProvisioning` record
bound to the authenticated email, retaining the existing pledge/appointment FKs.
A query nonce alone cannot establish another browser's provisioning context.

Existing operator-issued, seven-day, email-bound links remain supported through
`flask home_sace_bp provision-link --email <R-email> --issued-by <operator>`.
They are optional; sending R the standard URL requires no CLI command or admin UI.
Reading authority and Auditor invitations never substitute for HOME provisioning.

R signs the HOME IP pledge and registers/signs in, then claims HOME controller authority.
R can inspect provider documents, generate HOME Auditor codes, and inspect HOME
assignment evidence. Auditor codes expire after 14 days and are displayed once;
only hashes are stored. Codes start with HOME- and are not recognised by LITRE.

A enters a HOME code, signs the HOME evaluator pledge, registers/signs in, and claims
the invitation atomically under a row lock. Assignment access always checks the
signed-in Auditor. R cannot claim their own invitation. A controller can inspect
only assignments issued under their own HOME authority.

Pending session keys are prefixed `sace_home_`. Registration dispatch and explicit
HOME login continuation are the only shared authentication changes. Existing LITRE
branches remain unchanged. Returning users enter `/sace/home/`; HOME authority and
assignments are read from the database, not trusted from session role flags.

## Routes

All routes below are relative to `/sace/home`:

| Path | Methods | Purpose |
| --- | --- | --- |
| / | GET | Return to HOME controller centre or own assignments |
| /provisioning | GET, POST | Email-bound R entry and pledge |
| /authenticate | GET | Continue to shared identity registration/login |
| /control | GET | HOME Control Centre |
| /control/codes | POST | Issue HOME Auditor code |
| /control/documents | GET | HOME provider evidence versions |
| /control/assignments/<id> | GET | Issuing controller's HOME examination audit |
| /join | GET, POST | Validate HOME code |
| /pledge | GET, POST | HOME evaluator pledge |
| /claim | GET | Resume signed invitation claim after authentication |
| /ip-pledge | GET | Reopen this user's HOME pledge record |
| /assignments/<id>/board | GET | HOME evidence Board |
| /assignments/<id>/summary | GET, POST | HOME Summary / Understood |
| /assignments/<id>/experience | GET | Explicit not-yet-available experience framework |
| /assignments/<id>/materials/<kind> | GET, POST | Controlled material / examination confirmation |
| /documents/<version_id>/content | GET | Authorised, hash-verified PDF delivery |
| /assignments/<id>/completion | GET, POST | Outstanding HOME evidence and guarded completion |

All HOME responses use private/no-store caching and no-referrer policy. POSTs use the
platform CSRF protection. Opening a placeholder never creates examination evidence.
A document must first be opened; examination confirmation pins its exact version.
Publishing a later version makes it outstanding until that version is examined.
The Board has no PPP or Reading-style Demo.

## Controlled documents

Private files resolve beneath `SACE_HOME_DOCUMENT_ROOT` (default
`<instance_path>/sace_home_documents`). No existing storage configuration was changed.
No files were uploaded/copied into this root during implementation.

Operator publication command (not run against application data):
`flask home_sace_bp publish-document --controller-user-id <id> --kind <kind> --version <version> --storage-key <relative.pdf> --manifest <manifest.json>`

The command registers an already supplied PDF, never modifies old versions, validates
its private path/signature and records its checksum. Manuals additionally require a
complete HOME source snapshot and an explicit source-approval attestation recording
production parity, approver and approval date. Local unapproved snapshots are rejected.

Kinds: `timetable`, `participant_manual`, `facilitator_manual`, `assessment`,
`monitoring`, `certificate`. IP pledge is a separate HOME record, not a provider document.

## Manual source foundation

Source route: `app/subject_home/routes.py:chapter_page`, `/home/chapter/<chapter_num>`.
Models: `HomeChapter`, `HomeQuestion`, `HomeQuestionOption` in `app/models/home.py`.

- Section 1: chapters 1-10, `templates/subject_home/chapter1_practical.html` through
  `chapter10_practical.html`. Participant activities/preparation are embedded in
  templates; teacher-only expected answers/checklists support facilitator guidance.
- Section 2: chapters 11-20, `templates/subject_home/chapter_db.html`, with objectives,
  question text/types, answer keys and options supplied by the HOME database tables.
- Section 3: chapters 21-30, `templates/subject_home/chapter21_theory.html` through
  `chapter30_theory.html`. Includes database questions/objectives and template-based
  Theory Review modal explanations and summaries, which must be expanded in print.
- Images: `app/static/images`, resolved from Practical chapter image metadata and the
  existing route's filename fallbacks. All 30 chapters retain their actual titles.

The read-only builder foundation is `scripts/build_home_endorsement_manuals.py`:
`python -B scripts/build_home_endorsement_manuals.py --snapshot <private-output.json>`

It accepts only the explicitly verified localhost `ait_local_db`, uses a repeatable-read
read-only transaction, and emits deterministic source JSON and SHA-256, source text,
asset hashes, content gaps, builder version and two manual generation profiles.
It does not start the application or generate final PDFs. The snapshot includes
answer keys and is an internal build input, not a participant document.

Local inspection found all 30 chapters: 10 practical templates, 50 Application/MCQ
questions with 150 options, and 100 Theory questions with 200 options. No missing
chapter/objective/question/option/image was found by the implemented inventory checks.
This does not establish production parity or educational correctness of every answer.

The later print renderer must produce two manuals from the same approved snapshot:
participant-visible content only for the Workshop Manual; matching content with
source-linked brief guidance and existing teacher criteria for the Facilitator Manual.
Use deterministic question/option order, expand theory modals, preserve terminology,
and report unsupported/missing content. No LITRE manual is an input.

## Focused verification and limits

`python -B tests/test_home_sace_foundation.py` runs only HOME foundation checks in
connection-local temporary PostgreSQL tables, without calling create_app(). It uses
actual HOME models/routes and the existing shared authentication view functions.
Migration upgrade/downgrade runs in a separate connection's temporary namespace and
is rolled back. Existing LITRE sentinel data remains unchanged and only new HOME
objects are removed by downgrade. No migration stamp/public application tables change.

Checks cover new/returning R and A, new/existing-account registration, code generation,
invalid/expired/claimed codes, email-bound provisioning, pledge persistence, access
ownership, both directions of programme isolation (including one shared identity),
versioned document examination, unapproved manual rejection, real-layout rendering
and CSRF. Existing LITRE entry is checked without modifying its implementation.

Remaining work: approve production source parity; render and visually review both
manuals; supply HOME timetable/assessment/monitoring/applicable certificate evidence;
implement the actual HOME practical examination wrapper; confirm certificate
applicability and final HOME completion requirements. No qualification is issued by
this foundation and unavailable evidence cannot be marked examined.

No production access, commit, push or deployment was performed.

## Exact implementation file manifest

- `app/__init__.py`
- `app/auth/routes.py`
- `app/models/__init__.py`
- `app/models/sace_home.py`
- `app/program_sace_home/__init__.py`
- `app/program_sace_home/auth.py`
- `app/program_sace_home/cli.py`
- `app/program_sace_home/manual_sources.py`
- `app/program_sace_home/routes.py`
- `app/program_sace_home/service.py`
- `docs/home_sace_foundation.md`
- `migrations/versions/home_sace_001_foundation.py`
- `scripts/build_home_endorsement_manuals.py`
- `templates/program_sace_home/assignments.html`
- `templates/program_sace_home/audit.html`
- `templates/program_sace_home/authenticate.html`
- `templates/program_sace_home/board.html`
- `templates/program_sace_home/code.html`
- `templates/program_sace_home/completion.html`
- `templates/program_sace_home/control.html`
- `templates/program_sace_home/experience.html`
- `templates/program_sace_home/frame.html`
- `templates/program_sace_home/join.html`
- `templates/program_sace_home/material.html`
- `templates/program_sace_home/pledge.html`
- `templates/program_sace_home/provider_documents.html`
- `templates/program_sace_home/summary.html`
- `tests/support/home_sace_postgres_runner.py`
- `tests/test_home_sace_foundation.py`
