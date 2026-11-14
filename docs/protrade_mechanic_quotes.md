# Mechanic quotation register and individual item editing

Mechanic Debtor Register -> Quote opens the selected debtor's quotation register.
The register has Quote No., Date and Amount, with no Status/action column.
The quotation number opens its parts/labour register; each editable item has
one Edit link to a separate prefilled form. Save updates only that row and
returns to the same item register. Cancel does not save. Existing vehicle data,
VAT rate, unit cost, labour times, identifiers and numbering are retained.
The existing full creation form is reused. Old full-edit routes remain for
compatibility. A linked Quote/Rejected job-card URL redirects to the new item
register; Job Card opens retained operational functions separately.

PDF/email/public documents use the job's own shop. The PDF registration field
now uses the actual license_plate column instead of the nonexistent
registration_number attribute. Banking and existing document functions remain.
No email is sent automatically by individual-item corrections. Existing
Mechanic invoice-action token fees are retained; corrections charge zero.

## Essential migration

Revision: protrade_mechanic_quotes_012
Parent: protrade_debtor_identity_011

Adds nullable debtor_ledger.mech_job_card_id, a foreign key, unique constraint
and source validation. Existing tables and integer-cent balances are reused.
PostgreSQL triggers atomically synchronize one debit when a linked job's items
or VAT change, including saves through legacy compatible routes. Totals use
the existing parts quantity x markup price and labour hours x hourly rate,
then applicable VAT, rounded to cents. Payments stay credits.

The migration links eligible historical jobs using exact client/job references,
never customer names. A single known shop can be assigned to an unassigned
legacy account; an exact set of legacy job debits can establish a missing client
reference. Otherwise the historical relationship is left unguessed. A missing
account is created only for an unambiguous shop/client without a competing
historical charge. Existing IDs and quotation numbers are not changed.
One correctly valued legacy debit is adopted without rewriting its date,
description, reference or amount. Duplicate, mismatched or conflicting-account
charges abort migration for reconciliation rather than deleting/double charging.
Approval/deposit/invoice actions reuse the linked account and existing debit.

Run in Render Shell after deployment, before testing the updated workflow:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_mechanic_quotes_012')"
```

Applied successfully to verified local ait_local_db / ait_local. Original rows
and values were compared unchanged in thirteen accounting, vehicle, catalog and
numbering tables. The local database contains no mechanic jobs and no job 18;
legacy adoption is tested with real PostgreSQL fixtures using job ID 18,
Q-0001 and R989.37. Actual production job 18 remains unverified until deployment
and migration. Unresolved historical accounts require explicit reconciliation.

After migrating, check the production record without changing it:

```sql
SELECT j.id,j.shop_id,j.debtor_id,j.quotation_seq,count(l.id) AS ledger_debits,
       max(l.amount)/100 AS ledger_amount
FROM mech_job_cards j
LEFT JOIN debtor_ledger l ON l.mech_job_card_id=j.id
WHERE j.id=18
GROUP BY j.id,j.shop_id,j.debtor_id,j.quotation_seq;
```

## Focused verification

PostgreSQL and browser coverage includes job 18/Q-0001 register navigation,
single part/labour changes, VAT, other-item/vehicle preservation, stale/invalid
input, Cancel, reopening, owner/shop/debtor rejection, legacy debit adoption,
legacy account creation/linking, duplicate debit rejection, and existing
approval/deposit/tax-invoice actions. A new quote receives Q-0002 and one debit;
repeated save charges its normal creation fee only once. Existing PDFs are
rendered/read/visually inspected; email transport is mocked, never live.
The full suite retains Carpentry editing, payments, SOA, Output/folders and
concurrent numbering coverage.
