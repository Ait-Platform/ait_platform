# Reading/LITRE Auditor journey repair

The PPP is separate presentation/backup material: 31 slides, Previous/Next, then
Understood. It is not the forward-only Demo. Its existing evidence is unchanged.

## Historical cause and restored sequence

- `c0f89c03` established the competency survey at old online stage 33 and the
  four-question workshop post-test at old stage 34, after 30 presentation slides.
- `8868a05b` retained the four classroom-application checkboxes: Appropriate
  objective, Correct sequence, Practical demo and Reflection.
- `34ca9570` introduced assignment-scoped endorsement routes, replaced the survey
  with longitudinal-baseline questions, and introduced a separately configured
  Reading-course MCQ using the misleading name `AIT_READING_STEP35`.
- `37fc2adc` added actual presentation slide 31 to the already existing critique
  screen, leaving presentation and online evaluation on the same screen.
- `32a727b1` changed engagement text responses to checkboxes and added separate
  course/assessment/certificate links after the workshop test.
- `582a30e5` correctly removed the redundant introduction/back controls and refined
  Summary/PPP examination. Those correct changes are preserved.

Approved restored sequence:
**Slides 1-31 -> Critique 32 -> Classroom Application 33 -> Competency Survey 34 ->
Workshop Post-Test 35 -> existing Workshop Certificate outcome.**

The 18-video Reading course, its assessment and Reading Course Certificate are a
subsequent, separate journey. No questions or answers were invented.

## Routes, templates and stable evidence

| Visible stage | Route / template | Evidence and progression |
| --- | --- | --- |
| Slides 1-31 | `/sace/reading/simulator`, `endorsement_demo.html`; images through `/sace/reading/slide/<n>` | `demo_slide_1` through `demo_slide_31`; Save and Continue advances only the current server position |
| Critique 32 | simulator, `endorsement_demo.html` | Historical 0-3 vocalization/positioning/pacing radio ratings; existing `step31` evidence key |
| Classroom Application 33 | simulator, `endorsement_demo.html` | Historical four checkbox choices; existing `step32` engagement evidence |
| Competency Survey 34 | simulator, `endorsement_demo.html` | Historical eight competency ratings, 1-4; `workshop_survey` preserves named fields; existing `step33` completion key recorded only when absent |
| Workshop Post-Test 35 | `/sace/reading/step35`, `endorsement_workshop_mcq.html`; old `/sace/reading/post_test` remains compatible | Existing four questions, answers B/B/C/A, 25 points each, unchanged 70% threshold; existing `step34` result evidence |
| Workshop result | `/sace/reading/post_test/results`, `endorsement_results.html` | Pass records `demo_complete`; result exposes existing certificate delivery; failure retries Step 35 |
| Reading-course assessment | `/sace/reading/course/assessment`, existing `step35.html` file | Legacy configuration `AIT_READING_STEP35` and legacy `step35` course-result event remain for data compatibility; they do not define workshop Step 35 |

Historical evidence keys are deliberately retained so existing workshop eligibility,
certificate services, audit rows and completion calculations are not redefined by
presentation renumbering. Existing longitudinal-baseline responses are not deleted or
overwritten. New runs complete the restored competency survey. Old in-progress
positions resume at the corresponding activity; already passed workshops retain
eligibility. No schema/data migration or bulk rewrite is performed.

Workshop and Reading certificate generation, email delivery and eligibility functions
remain the established mechanisms. The survey remains separately recorded evidence;
its numeric ratings are not inserted as fabricated competencies into certificate PDFs.

## Auditor Board: authoritative order

1. Activity Summary - `/sace/reading/auditor-map`
2. Application Form 1 - `/sace/secure_view/app_form`
3. Application Form 2 - `/sace/secure_view/app_form_2`
4. Facilitator Manual - `/sace/secure_view/f_guide`
5. Participant / Workshop Manual - `/sace/secure_view/p_guide`
6. AIT IP Pledge (reference) - `/sace/secure_view/ip_pledge`
7. Reading Timetable (T/T) - `/sace/secure_view/timetable`
8. PPP: examine all 31 slides - `/sace/reading/presentation`
9. Forward-only Demo: 31 slides and workshop interactions - `/sace/reading/simulator`
10. Evaluation and Assessment - `/sace/reading/step35`
11. Workshop Certificate evidence - `/sace/reading/post_test/results`
12. 18-video Reading course - `/sace/reading/course`
13. Reading Course Certificate evidence - `/sace/reading/course/certificate`

The obsolete Board paragraph is removed without replacement. Summary Understood
still records `map_reviewed`; PPP Understood still uses existing PPP evidence.
Item 10 reflects the workshop evaluation result, not the independent course MCQ.

## Reading-course configuration findings

The legacy key is read from Flask configuration in `reading_mcq()` and in
`endorsement.step35_passed()`. The expected mapping contains a version string,
pass_percent (1-100), and questions with unique id, prompt, options and answer.
The score is calculated server-side; revised versions invalidate old-version passes.

Tracked source/history searches found its introduction and later validation changes,
documentation and synthetic test fixtures, but no approved production question set
or seed. Local `.env` and `config.py` do not mention this key. No secret values were
read into the report and no production configuration was accessed. Reading-course
assessment therefore stays unconfigured in the inspected environment; no questions
or pass are fabricated. Workshop Step 35 uses the actual historical workshop MCQ.

The Reading certificate still requires all 18 video completions and a current-version
course-assessment pass. That missing course content can still block the Reading
certificate/final assignment closure, but it no longer blocks workshop completion.

## Focused verification

- `python -B tests/support/sace_reading_repair_runner.py`: six passing checks using
  actual routes and temporary local PostgreSQL evidence; no application factory.
- `python -B tests/test_sace_journey.py`: nineteen passing focused unit/render checks.
- Checks cover exact Board order/links, the actual manual/timetable PDF responses,
  Summary and PPP evidence, every slide and online transition, no skips/replays,
  invalid form protection, workshop pass/failure, old evidence/eligibility preservation,
  separate course gates, and no pass for missing course questions.
- Certificate mail behaviour is checked with mocks only; no real email was sent.
- Thirty-four protected HOME/shared/manual source files were hashed before the repair
  and verified unchanged afterward. Manual PDFs and slide content were not rewritten.

No HOME work, provisioning, media/storage, UIP or billing code was changed. No
migration, commit, push or deployment was performed.
