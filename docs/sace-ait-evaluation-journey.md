# AIT activity evaluation journey

SACE supplies and controls its own external endorsement rubric. No SACE rubric,
endorsement criteria, or endorsement decision is implemented by this journey.

The existing SACE URLs now use assignment-scoped evidence in
`sace_workshop_interactions`. Each access-code row is locked for state changes.
The Auditor Board records the map, controlled document confirmations, all 30 PPP
slides, the forward-only Demo, AIT testing steps 31?34, workshop certificate mail
acceptance, the 18 Reading videos, Step 35, Reading certificate mail acceptance,
and return to the Board. Reading progress belongs to the assignment and does not
change a separate paid learner enrollment.

Step 31 is the existing F presentation critique. Step 32 records P engagement
responses. Step 33 exercises the existing longitudinal study baseline; this is
explicitly test evidence, not longitudinal outcome data. Step 34 uses the existing
four workshop questions and threshold. Video evidence records content serving and
browser end-of-video confirmation; it does not claim independent proof of attention.

## Content readiness

The Facilitator Manual must be the actual registered `SaceDocument` of type
`f_guide` for slug `reading`, or `app/static/pdf/F_Guide.pdf`. A timetable is not a
substitute. That fallback file is absent in the inspected checkout. The application
and participant guide use their registered documents or existing static PDFs.
Unavailable materials never receive completion credit.

Step 35 stays pending until provider course content is supplied. Set the Flask
configuration key `AIT_READING_STEP35` to a mapping containing `version` (a nonempty
string), `pass_percent` (integer 1?100), and `questions` (nonempty list). Each question
contains a unique string `id`, its `prompt`, an `options` mapping of option keys to
labels, and an `answer` key that exists in those options. No production questions
or threshold are invented. The answer key stays server-side. Change `version` when
publishing revised content; an old-version pass cannot complete the journey.

## Completion and R audit

Completion records, in one transaction, `evaluation_complete`, a durable
`controller_notification` addressed to the provisioning R, and `assignment_closed`,
then sets the assignment to `Completed`. The notification says exactly:
"A has completed the AIT activity evaluation journey."
The R Control Centre displays this notification and provides the complete CSV
interaction audit for R's own assignments. No email to R is sent by this action.
A repeated completion request is idempotent. Closed assignments cannot re-enter
through SACE or legacy Reading routes. No evidence is deleted by closure.

Certificate requests use the existing standard PDF generators and email helper.
Suppressed, failed, or unconfirmed mail delivery cannot satisfy completion.
No email is sent by development tests.

## Verification

Run `python -B tests/test_sace_journey.py`. These focused tests execute actual
function definitions with mocked persistence and render the new Jinja templates.
They intentionally avoid the application factory, which performs database writes
at startup. PostgreSQL transaction/concurrency behavior, actual video/PDF delivery,
and live email delivery still need integration verification in the test deployment.
No database schema changes are needed.
