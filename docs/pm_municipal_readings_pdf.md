# Municipal Meter Readings PDF and Municipal Reference

The existing Meter Readings workflow includes Generate Municipal Meter Readings
PDF. Its selected billing month is used for the entire authenticated user's
authorized property portfolio (the same bil_property.manager_id relationship
used by existing property access). The selected property is checked before
export; every exported reading and account association is independently scoped
to the user's portfolio.

Electricity and water are separate A4 tables with Account, Reference, Meter,
Reading and Captured columns. Each recorded reading dated within the selected
calendar month appears with its saved value and date. Captured is an empty
manual checkbox. No prior/next-month reading, consumption, opening estimate,
or METSOA cache is substituted. Missing or conflicting account associations
stop generation with a review message. An empty period is explicitly shown.
The last page reserves handwriting lines and a municipal stamp box.

Municipal Reference is an optional nullable String(100) on bil_meter, separate
from meter_number. The existing Municipal Accounts meter editor can save and
clear water/electricity references; lookup and edit reload the saved values.
Blank references stay blank in the PDF, without an inferred E/W reference.
Stale-form checks include reference metadata. Readings and historical meter
numbers are never rewritten by a reference edit. Setup Wizard is unchanged.

## Migration

Forward-only revision pm_meter_reference_001 follows pm_muni_arrangements_001.
It adds only bil_meter.municipal_reference, nullable, with no backfill or deletes.
No production migration has been executed in this development task.

After separate production migration approval and the established Render database
identity checks, apply the targeted revision in the service Shell:

    python -m alembic upgrade pm_meter_reference_001

Apply the migration before using Municipal Reference or generating this report.
The model field is deferred so ordinary legacy meter loads do not select the
new column before deployment of the migration.
Until the column exists, reference saves return a clear migration-required
message. The PDF still exports recorded readings with blank Reference cells. Existing reference-free SQL account workflows remain
compatible. Review the production revision chain before applying the command.

## Validation

Local PostgreSQL temporary-table migration and metadata round-trip tests;
portfolio, date-boundary, empty-period and association isolation checks;
read-only export query monitoring; real A4 PDF content, blank-reference,
pagination and receipt checks; Poppler-rendered short and long synthetic proofs
visually inspected. Existing arrangement, account, capture and filter workflows
are tested separately.

## Reporting period investigation (3 November 2026)

A read-only inspection found that the existing Meter Readings table displayed
all history for the selected property, while municipal PDF selection correctly
used recorded dates within the selected month across the authorized portfolio.
The external September submission contained more readings than the database:
the inspected portfolio had no September electricity rows and three September
water rows, with additional rows dated October. Several submitted meter
identities were absent and some saved meter/account identities differed from
the submission. No document values were imported and no dates or links changed.

The page now provides an explicit reporting-month selector and property versus
portfolio period counts; its historical table remains clearly labelled.
The PDF no longer blocks recorded readings when the optional reference column
has not yet been migrated; those Reference cells remain blank. It never falls
back to a neighbouring month's readings or a document's reported values.
