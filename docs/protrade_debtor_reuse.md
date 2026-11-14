# ProTrade debtor reuse and first quotation fixes

## Behavior

- `/mechanic/quote/new` uses the module-level MechJobCard and MechVehicle
  imports. The later local import no longer shadows them before draft lookup.
- Add Debtor includes a searchable owner-only saved-debtor picker. The server
  resolves the selected identity and copies its saved contact details; posted
  replacements cannot change the selected identity's information.
- Existing Debtor rows are the existing shop-specific accounting accounts.
  Reuse creates one account for the selected shop, linked by identity_debtor_id
  to the original debtor identity. It does not move or merge old accounts,
  quotations, payments, opening balances or financial history. A linked shop
  starts at R0.00. Existing IDs remain unchanged. Contact edits remain local
  to the shop account and do not rewrite saved quotations.
- Repeated selection returns the existing account. Row locks and a unique
  owner/shop/identity index protect concurrent linking. The database also
  rejects cross-owner identities, identity chains and linked-account moves.
  Existing unlinked legacy identities are not guessed or merged.
- Quote and Ledger are active links for every saved shop debtor. As explicitly
  authorised for Dick's other trade, non-mechanic trades reuse the existing
  non-vehicle quotation builder, register and individual-item editor. A
  quotation-only WORK-001 General services template starts without prices;
  description and rates can be entered for the quotation. The Price List is
  unchanged. Carpentry retains its existing templates and pricing behavior.
- These quotations use the existing atomic quote-debit sync, exact-money
  payment/SOA paths and numbering allocator. No new accounting tables,
  calculations or token charges were introduced. Mechanic handling is retained.

## Essential production migration

Revision: `protrade_debtor_identity_011`
Parent: `protrade_payment_method_010`

Adds a nullable identity link, uniqueness index and ownership guard to debtor.
Allows non-mechanic trade quotations in the existing quote table and extends
only the trade predicates/description in the existing ledger sync functions.
No existing debtor, financial, pricing or numbering records are rewritten.
The migration fails safely if the expected existing ledger functions differ.

Run in Render Shell using the deployed service configuration before exercising
these screens (this also applies any missing predecessor on this branch):

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_debtor_identity_011')"
```

Applied to verified local `ait_local_db` / `ait_local`, with original rows and
values compared before/after in ten accounting/catalog/numbering tables.
Production migration and live behavior have not been verified locally.

## Verification

98 focused PostgreSQL/browser tests passed. Coverage includes the actual local
imports in new_quote (the older harness strips imports), debtor search/linking,
repeat link protection, cross-owner/shop access rejection, separate balances,
Dick's empty ledger and first non-vehicle quote, quotation-specific descriptions,
item editing, payments/SOA, existing mechanic token behavior and Carpentry flows.
The isolation assertion now checks the shop register rather than the entire
page, since the owner-only reuse picker intentionally includes other shops'
identities.
