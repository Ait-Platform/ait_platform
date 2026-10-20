# Property banking register migration

Production execution requires separate explicit approval. This change does not execute or stamp any migration.

After confirming the production identity and taking the approved backup, review and apply only Alembic revision `pm_bank_register_002` (parent: `pm_lease_payments_001`). Do not run a blanket upgrade of all heads.

The migration drops only `ck_pm_payment_allocation` after verifying its existing definition. It changes no rows, flags, bank accounts, foreign keys, unique property/account pairs, draft JSON or signed snapshots. Both allocation columns remain non-null. New links explicitly store false/false; selecting an existing link preserves its flags. Roll back application code if needed; destructive downgrade is disabled.

Before this migration is applied, the banking register remains visible but additions and links are blocked with a migration notice. Historical banking particulars are never imported automatically. Central account selection lists bil_bank_detail; associated property names are restricted to the signed-in manager's active portfolio. Existing property authorization and sanitized integrity logging remain in place.
