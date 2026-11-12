# Stage 3E: trade-specific register quotations

Client Register shop rows show Edit followed by Quote for Mechanic and Carpentry.
Ordinary customer rows retain View Client and now have Edit (existing update
handler) followed by Quote. A shop row uses its own registered trade and Price
List. An ordinary customer uses the explicitly selected shop and its registered
trade/Price List, as confirmed by the user. Missing shop selection returns to the
dashboard with a selection prompt. No customer registration per shop is required.

Mechanic dispatch opens the existing quote form and save pipeline. Only selected
client details/prefill and hidden customer/shop context were added to that form.
The selected existing customer ID is retained when saving, avoiding a duplicate
customer when a display name differs. New mechanic job cards record shop_id;
vehicle fields, quotation calculations, token handling and invoicing remain as
before. Existing mechanic records keep all old field values and NULL shop_id.

Carpentry uses the same register and Price List infrastructure but a vehicle-free
BOQ quote form. The existing mechanic job-card model requires a vehicle, so BOQ
quotes use protrade_boq_quotes rather than fabricating vehicle records. The form
shows Client Details, BOQ items, quantities, per-unit rates and totals. Quotation
quantities default to 1 independently of Price List quantities. The server reads
rates/items from the selected owned shop's Carpentry Price List and computes each
line amount with Decimal, rounded to cents; grand total sums those line amounts.
Submitted rates, totals or out-of-scope item IDs cannot override the server's
values. No tax or new payment rules are introduced by this stage.

Saved quotes link to owner, shop, trade and (for ordinary customers) customer ID.
Client contact details, codes, descriptions, units, rates, quantities and totals
are snapshots. Subsequent Price List or customer edits do not rewrite old quotes.
Saved quotes can be reopened from the form's recent quotes list. A per-form UUID
prevents duplicate records on a repeated successful submission. Every new read
and save validates owner/shop/trade. Other trades and invoicing are not modified.

## Verification

47 distinct focused checks passed: 10 quotation, 7 quantity, 7 predefined catalog,
6 BOQ, 6 Price List, 6 scope migration and 5 register checks. A combined 46-test
run passed before the final additional multi-shop customer check; that check and
subsequent targeted quotation/register reruns also passed. Tests use isolated
PostgreSQL transactions and the actual routes/templates/models/migrations. Browser
checks cover item/quantity selection and live line/grand totals. Tests include
mechanic save/token compatibility, no vehicle fields or dummy vehicles for
Carpentry, exact client links, multiple shops sharing one customer, repeat saves,
invalid quantities, snapshot preservation and cross-owner/shop item rejection.

The actual targeted local Alembic upgrade succeeded on verified ait_local_db.
All original mechanic job-card fields, BOQ records and mechanic catalog values
compared exactly before and after. git diff --check passed.

## Required production migration

Apply protrade_quote_005 against Render's assigned runtime DATABASE_URL before
the updated application serves dashboard/quote requests (for example in Render's
pre-deploy phase). It depends on protrade_boq_004, creates protrade_boq_quotes and
adds nullable mech_job_cards.shop_id. It does not rewrite or infer old shop links.
Do not use upgrade heads: unrelated legacy migration branches exist.

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_quote_005')"
```

Verify the migration before dashboard testing:

```sql
SELECT version_num FROM alembic_version WHERE version_num = 'protrade_quote_005';
SELECT to_regclass('public.protrade_boq_quotes');
SELECT column_name FROM information_schema.columns
WHERE table_schema='public' AND table_name='mech_job_cards' AND column_name='shop_id';
```

Production migration execution has not been verified in this task. Credentials
were not taken from historical scripts or committed. Downgrade refuses destructive
changes to saved quotation data.
