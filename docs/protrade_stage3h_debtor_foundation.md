# Stage 3H: shop-owned Debtor Foundation

The consultancy client is a shop. Its debtors are external customers of that
shop, not people who must register separately with AIT. The dashboard Client
Register now lists shops only; selecting one opens its own Debtor Register.
Legacy global customer rows are retained in the database and legacy screens.
Both same-named shops with different registered trades remain separate records.

Menu: Price List | Debtor Register | Tokens | Setup. Every link keeps explicit
shop context. Quote / Invoice is removed from the menu. Legacy document/account
registers remain linked from the selected shop's Debtor Register.

Debtor capture/edit saves name, phone, email and address against both owner and
shop. A shop-scoped MechClient contact supports the existing mechanic quote
pipeline, linked by Debtor.reference_id; it is not a consultancy-level client.
Exact repeated contact submissions reuse the existing debtor in that shop.
Different shops retain independent debtor/contact records and documents.

New quotations require a saved debtor belonging to the selected owned shop.
The old shop/client Quote entry points redirect to debtor selection without
creating quotes. Debtor Quote opens the shop's registered trade and Price List.
Carpentry uses BOQ items and snapshot rates; the mechanic path retains its vehicle
quote form, calculations, billing and token rules.

Draft identity is explicit and persisted. Opening or reviewing never inserts a
quotation. Save creates one record; subsequent saves update the current draft.
Carpentry drafts preserve saved BOQ rates while quantities are edited. GET never
rewrites debtor contact snapshots. An explicit draft save refreshes contact
snapshots from its saved debtor. Mechanic Edit posts to the existing edit route,
not the new-quote route; reopening/saving an existing draft incurs no new charge.

Only an explicit New Quote POST closes the prior open draft and starts a fresh
form; that POST/GET still creates no quotation until Save. The previous quote
number, items and total are preserved. Closing a draft is not acceptance,
sending, billing or conversion. Partial unique indexes guarantee at most one
open draft per debtor/shop; debtor row locks serialize saves and New Quote
requests. Repeated stale submissions cannot create extra draft rows.

Migration protrade_debtor_006 is additive and depends on protrade_quote_005:
- debtor: nullable shop_id and address;
- mech_clients: nullable shop_id;
- BOQ quotes and mechanic job cards: nullable debtor_id and is_draft;
- partial unique indexes for new, explicitly linked open drafts.

No historical assignment, draft classification, merge, deletion, quote/invoice
renumbering or BOQ/parts price rewrite occurs. Existing NULL-linked documents
remain readable through the legacy links. Existing invoices remain unchanged.
Sending, acceptance, invoice conversion and trade work-order development are
not implemented by this stage.

Verification (2026-11-12): a combined 65-test PostgreSQL/browser suite passed,
then final focused reruns passed with the added contact-preservation check:
66 distinct checks overall. Tests cover capture/edit, owner/shop isolation,
shop-first UI, repeated opens/saves, explicit new documents, database draft
uniqueness, legacy migration preservation, browser form values, quote totals,
and mechanic in-place editing without repeated wallet charges.

Actual targeted Alembic upgrade succeeded on verified localhost ait_local_db.
Every original field in debtor, mech_clients, mech_job_cards, mech_invoices,
protrade_boq_quotes, protrade_boq_items and mech_catalog_parts compared exactly
before and after. git diff --check passes.

## Required Render migration

Run before the updated service serves requests, using the assigned Render
DATABASE_URL (for example in its pre-deploy phase). Do not use upgrade heads.

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_debtor_006')"
```

Verify the revision and indexes before testing:

```sql
SELECT version_num FROM alembic_version WHERE version_num='protrade_debtor_006';
SELECT indexname FROM pg_indexes WHERE schemaname='public'
AND indexname IN ('uq_protrade_boq_open_draft','uq_protrade_mech_open_draft');
```

Production execution has not been verified in this task. Current Render resource
configuration is the authoritative credential source; no historic credentials
were used or stored. Migration downgrade refuses destructive removal of links.
