# HOME Phase 2B Auditor examination wrapper

Implemented locally against frozen Phase 2A commit
`2307c8b1e2f158cc1fd6dc083899c6a037b69dd7`. No authority/lifecycle, shared login,
Reading, participant route/template/model, migration or finalizer changes were made.
No production access, deployment, commit, push or final PDF generation occurred.

## Board and requirements

New HOME claims use `home-auditor-v2` requirements by default:

1. Activity Summary
2. Application Form 1
3. Application Form 2
4. HOME Programme / Timetable
5. HOME Participant Manual
6. HOME Facilitator Manual
7. HOME Learning Journey
8. Assessment / Evaluation
9. Workshop Evaluation / Monitoring
10. Certificate / Diagnostic Report Evidence

Existing assignment requirements are never rewritten. Historical
`home-foundation-v1` assignments retain the original eight items and placeholder
experience. They cannot use Phase 2B confirmation endpoints. The foundation test
harness explicitly selects historical requirements; its assertions and the frozen
Phase 2A lifecycle suite are unchanged. `SACE_HOME_REQUIREMENTS_VERSION` can select
historical requirements for an isolated fixture, without modifying stored rows.

## Immutable private content

`scripts/build_home_auditor_snapshot.py` bypasses the application factory, accepts
only verified localhost `ait_local_db`, and reads educational content in a
repeatable-read/read-only transaction. It uses the existing source inventory plus
HOME assessment/certificate template sources. No participant records are read.

The bundle contains all 30 chapter identities, objectives, pass marks, ordered
questions/options/answer keys, source hashes, inert educational HTML, fixed
assessment selection, specimen layouts and the exact image bytes/hashes. Chapter
numbers resolve chapter identity; IDs are never assumed to equal chapter numbers.
Question order is by ID and option order is by sort order then ID.

Canonical JSON SHA-256 identifies the complete manifest, including source approval
reference and rendering output. Private files are named `<sha256>.json`; existing
versions cannot be overwritten by the writer. `CURRENT` selects the version for
previously unbound examinations. First use binds a version through HOME evidence,
under the existing HOME subject lock. Later selection/content/database changes do
not change an assignment's bound examination. Every load validates the manifest,
inert markup, identities, fixed question selection and image bytes/hash.

Configuration: `SACE_HOME_EXAMINATION_ROOT` defaults to
`instance/sace_home_examination`. `SACE_HOME_EXAMINATION_VERSION` optionally selects
a hash instead of `CURRENT`. Neither JSON nor answer-key bundles are exposed by a
public route. Images use assignment-authorized private routes. Missing/corrupt
bound versions fail closed. Retain older files while any assignment references them.

The builder requires `--approved-by` and `--approval-reference` as provenance.
Local implementation verification does not assert production parity or substitute
for provider review. It creates no manuals, application forms or PDFs.

## Learning Journey

`/sace/home/assignments/<id>/experience` opens the four-stage chapter index:

- Practical: chapters 1–10, existing preparation, instructions, images, expected
  responses and final criteria.
- Application / MCQ: chapters 11–20, existing database questions/options and answer
  evidence; no participant answer submission or grading.
- Theory: chapters 21–30, objectives, expanded explanations/examples/summaries and
  the existing knowledge-check questions.
- Review / Retention: the same existing Theory Review and knowledge checks, with
  a separate examination acknowledgment; no new question bank.

Chapter endpoints are
`/sace/home/assignments/<id>/experience/<stage>/<chapter_number>`.
The offline renderer uses a standalone Jinja environment and a markup allowlist.
It removes forms, input controls, buttons, participant links, scripts, event
handlers, modal hiding and unsafe image references. It never imports/calls the
participant routes. Expected-response material is deliberately present as protected
endorsement evidence.

Each page records opened and examined evidence with exact version, stage/chapter
identity, chapter hash, question IDs and ordered option IDs. All 40 stage/chapter
acknowledgments are required for Learning Journey completion. Partial coverage
cannot satisfy it. Evidence is idempotent and posted version mismatches are rejected.

## Assessment and certificate specimens

`/materials/assessment` presents the first five questions by question ID from each
chapter number 21–30: a declared fixed 50-question specimen. Exact content/selection
and existing scoring criteria are in the manifest. Existing criteria are exact
answer matching, complete multi-select set matching, category percentages, the mean
of ten categories and a participant threshold of 70%. The Auditor is not graded.

`/materials/certificate` presents both existing passed/failed reporting layouts,
rendered offline using `Sample HOME Learner`, `HOME-SPECIMEN-NONISSUED` and a fixed
sample date. Sample category scores are 80%/60% respectively. Specimen/non-issued
banners are prominent; no official logo or seal is rendered. The original
non-academic-qualification disclaimer is retained. Auditor identity is never passed
to this renderer. No assessment, certificate issuance/email, qualification,
enrollment change or participant pass/fail result occurs.

## Provider documents remain missing

Slots exist for HOME Application Forms 1/2, timetable, both manuals and workshop
evaluation/monitoring. No substantive missing document was authored or fabricated.
Until an approved HOME version exists, the Board shows unavailable/incomplete and
confirmation fails. All six documents remain required even after educational
examination is complete.

For Phase 2B a PDF manifest must contain `subject: sace_home_endorsement`, the exact
`kind`, and `home_approval` with `approved_by` and `reference`. Existing stricter
manual source-approval requirements also apply. The known frozen Reading application
forms, both guides and timetable are rejected by content hash even if renamed or
labelled HOME. Provider approval remains necessary for unseen documents; the hash
denylist is not a substitute for substantive document review.

First opening pins the approved document version in HOME evidence. PDF hash and
complete document metadata/source-manifest hash are bound; later versions cannot
replace it and changes to bound metadata are rejected. No schema change is needed.

## Verification

- Phase 2B support suite: 11 tests passed, covering all 30 chapters, all 40 stage
  acknowledgments, exact Board order, missing documents, fixed specimens,
  deterministic builds, nonmatching chapter IDs, pinned revisions, tampering,
  ownership, CSRF, deadlines/revocation, protected assets and complete examination.
- SQL instrumentation permits wrapper mutations only to `sace_home_*` tables,
  rejects participant/content table access, and verifies enrollment/Reading records
  unchanged. Participant routes and mailer are not imported.
- Synthetic document fixtures demonstrate complete examination; they are not
  substantive provider documents and are removed with isolated temporary tables.
- Frozen Phase 2A lifecycle: 19 passed.
- HOME foundation: 14 passed.
- HOME continuation: 10 passed.
- Reading/HOME isolation: 24 passed.
- Discoverable Phase 2B entry point: one wrapper test passed, running the same 11
  checks in an isolated subprocess.
- Scoped diff whitespace check passed; frozen authority, lifecycle, participant,
  Reading and migration paths have no changes. Index remains empty.

Initial foundation checks exposed two legacy-requirement expectations when new
claims acquired Phase 2B requirements. Explicit historical requirements in the
foundation fixture resolved both without changing authority or test assertions.
Reading's existing `datetime.utcnow()` deprecation warning remains. Full repository
testing and production parity checks were not performed.

## Commit manifest after review

- `app/program_sace_home/content_snapshot.py`
- `app/program_sace_home/examination.py`
- `app/program_sace_home/routes.py`
- `app/program_sace_home/service.py`
- `scripts/build_home_auditor_snapshot.py`
- `templates/program_sace_home/board.html`
- `templates/program_sace_home/journey.html`
- `templates/program_sace_home/examination_chapter.html`
- `templates/program_sace_home/specimen.html`
- `templates/program_sace_home/_questions.html`
- `templates/program_sace_home/_examine.html`
- `tests/support/home_sace_postgres_runner.py`
- `tests/support/home_sace_examination_runner.py`
- `tests/test_home_sace_examination.py`
- `docs/home_sace_phase2b.md`

Private local verification bundles and `CURRENT` under ignored `instance/` are
runtime artifacts, excluded from the proposed source commit. Review/approve the
content source and privately provision a suitable bundle in a later authorized
operational step. No production work is authorized by this implementation.

Local `CURRENT` selects
`7321ea468db7916658534d7630d6be6f20b2f20c27c3d0c0c18fb7229986fb07`.
An earlier local verification bundle
`e5af5c5ed180d4016f163956c2d9152f6a88a25c310b5c139963f9b0d86c252a`
is retained separately, demonstrating append-only version storage. These are
implementation-verification artifacts, not production-approved provider documents.
Each bundle is approximately 18 MB including its immutable embedded images;
runtime currently validates on every load rather than relying on a content cache.

Suggested commit: `Implement read-only HOME Phase 2B Auditor examination evidence`.
