# ProTrade Stage 3A: shop and trade price-list scope

Implemented only Stage 3A. The dashboard menu says Price List; the management
page says Manage Price List. The selected registered trade is displayed on the
dashboard and management page, and Price List links/forms retain the shop ID.
Explicit IDs are checked against the authenticated owner. Posted trade values
cannot override the shop's registration.

`mech_catalog_parts` gains nullable `shop_id` (FK) and `trade` columns. New custom
prices carry both. Reads, updates and deletes apply the same shop/trade scope;
new/edit quote price selections reuse the same filter. Global entries remain
read-only defaults for mechanic shops. NULL-scope legacy owner prices are used
only by that owner's first mechanic shop (lowest existing shop ID), retaining
legacy custom-over-global pricing. No historical records are copied, rewritten,
reassigned or deleted by the upgrade. Multiple mechanic shops share global
defaults but have separate custom prices. If an installation had multiple
mechanic shops before scoping, review legacy custom-price attribution before
manually moving any historical entries; this change does not guess a backfill.

Deployment: `app/protrade_price_schema.py` performs a repeatable additive upgrade
in one transaction after independently checking the configured database/user.
Normal application startup calls it inside the existing `SKIP_AUTO_MIGRATE`
guard, independently of the repository's multiple legacy Alembic branches.
Operator-mode startup does not mutate schema. If automatic migration is disabled,
run this targeted helper against the service's authoritative configured database
before starting the updated application. No unrelated schema migrations are
required by Stage 3A. Production uses the running Render service connection, not
credentials from historical scripts. Production deployment is not verified here.

Verification (2026-11-12):
- `.venv/Scripts/python.exe tests/test_protrade_price_list.py`: 11 PostgreSQL
  tests including five Stage 1 regressions; temporary tables rolled back.
- Schema upgrade tested twice against legacy rows, preserving every original
  field. Applied to verified localhost `ait_local_db`; all six existing catalog
  rows and their prices matched exactly before and after.
- Changed Python files parse; `git diff --check` passes.

No BOQ items, sample prices, quantities, materials/labour engine or Carpentry BOQ
module are included. Stage 3B remains deferred.
