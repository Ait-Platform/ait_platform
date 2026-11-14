# ProTrade SOA and payment methods

The Carpentry statement retains existing calculations and uses A4 table widths
15% Date, 43% Details and 14% each for Debit, Credit and Balance. Monetary
columns remain right aligned with two decimal places. PDF verification covers
the supplied R2,148.32 balance and a single page without wrapped descriptions.

Record Payment now offers Cash, EFT, eWallet, Card and Other, plus an optional
Transaction Reference. Existing descriptions and references are preserved;
historical methods are not guessed or rewritten. Existing duplicate protection
is retained, including recognition of older method-as-reference submissions.
Mechanic payment handling is unchanged.

Only the debtors program entry is hidden from the main navigation. Debtor,
ledger, statement, registration and payment routes remain available.

## Required production migration

Revision: protrade_payment_method_010
Parent: protrade_quote_ledger_009

Adds nullable debtor_ledger.payment_method and adjusts payment validation and
uniqueness for optional references. It does not rewrite existing accounting
rows. Tested and applied to verified local PostgreSQL with original rows and
values checked before and after. Production application remains pending.

Run in the deployed service's Render Shell before using the updated payment form:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'protrade_payment_method_010')"
```

Focused PostgreSQL/browser tests cover methods, optional and legacy references,
repeat submissions, isolation, unchanged wallet/quotation counts, PDF contents
and geometry, and navigation-only removal. Live production behavior has not
been verified by these local tests.
