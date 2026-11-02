# Tariff Management delivery

## Scope

The existing `/admin/billing/tariffs` list now links to separate new, edit and history pages. Only authenticated administrators may use these pages. Existing global CSRF protection remains enabled. The old list POST actions (including overwrite and deletion) return 405.

All five existing UI classifications remain available. New identities require a verified effective date. Units are explicit, tier boundaries and allocation factors are validated, and existing metadata is copied unchanged for new rate versions, including NULL values and 999 factors. The list selects the currently applicable rate using the already verified resolver.

## History and concurrency

No migration is required. `bil_tariff` already stores version rows with start dates. Each identity is utility type plus normalised code. All application tariff writers now acquire the same PostgreSQL transaction advisory lock before checking and inserting. Independent-session tests verify duplicate identity/version rejection. No historical row is updated or deleted. The writer verifies PostgreSQL READ COMMITTED isolation before proceeding and fails safely for another isolation level. External tools must not bypass the application writers.

New versions must start after every existing version and cannot be backdated into past billing periods. The edit form carries the latest stored row ID; concurrent edits are rejected with entered values retained. The next start supersedes the previous version. History displays the preceding inclusive end date (next start minus one day). Scheduled rates are never labelled Current. Missing dates are labelled Unknown, not manufactured; ambiguous records block editing.

## Verification and release

Focused tests use local `ait_local_db`, session-temporary tables and a randomly named disposable table for two-connection concurrency tests. They never write production tariffs. Browser coverage exercises list layout, stacked actions, conditional fields, creation, version editing and history. A production-shaped 13-row fixture verifies original rates, effective dates and factors remain unchanged. Existing tariff consumers, METSOA HTML/PDF and Utilities regressions are run separately.

The owner's prior production audit confirmed 13 unique identities, valid effective dates and no pre-existing historical versions. This task does not re-audit production or change production records. No Setup Wizard, billing formulas, financial records or other programme workflows are modified. Render deployment and authenticated live verification must be confirmed separately after the pushed commits deploy.
