# Simplified account-based Franchise registration -- revision 003

## Application changes

Registration and editing collect full name, cellphone, residential address and
optional occupation. Once-off registration still requires terms acceptance.
There is no SA ID or email textbox, no ID checksum/duplicate check and no new
identity ciphertext/digest. Email and user_id come exclusively from the signed-in
AIT account, including profile updates. Posted identity/email overrides are ignored.

Historical identity columns and values remain untouched and can still be viewed
by authorised administration with the original keys. Profile editing reads only
the saved address, never the historical identity. Identity-key loss cannot block
editing an independently readable address. New address writes retain existing
versioned AIT protection. Account/email/registration-number uniqueness, CSRF,
client isolation, suspension, historical earnings and programme gates remain.

A successful commit redirects to the register, where the saved row is immediately
visible. Subsequent visits retrieve that row; Edit updates it without assigning a
new number or registration date. Invalid fields and failed commits remain visible.

## Why a migration is required

The published 001 constraint `ck_franchisee_submitted` requires both
identity_ciphertext and identity_digest for submitted/active records. Merely hiding
the input cannot allow a real NULL identity to be inserted. The old identity_digest
unique constraint also implements the duplicate identity rule being retired.

New forward-only revision `franchise_registration_003` follows
`franchise_registration_002` and performs only:

1. Replace `ck_franchisee_submitted` with the same full-name/cellphone/address/
   submitted-at/invitation conditions, omitting the two identity NOT NULL conditions.
2. Drop the single unique constraint on identity_digest and its backing index.

No data/column/ID/number/date/ciphertext/history/owner/FK/sequence value is changed.
The user_id, email and franchisee_number unique constraints remain. The existing
identity columns remain nullable and stored historical values are preserved.
The migration refuses an unexpected identity unique-constraint shape. It requires
`franchise_registration_simplify_approved=True` in its explicitly approved Alembic
context. No published migration has been modified.

## Production execution is NOT authorised

Verify the recorded identity, effective permissions, all recorded Alembic heads,
physical schema and a current recoverable backup before requesting execution
approval. The required order is:

`ait_franchisee_001` (already applied) -> `franchise_registration_002` ->
`franchise_registration_003`.

If 002 has already been applied, only 003 should be outstanding. Otherwise both
002 and 003 need explicit approval. Determine this from ancestry across all heads;
do not stamp, manually create columns or use upgrade heads. The existing 002-only
runner does not authorise or execute 003. A final guarded execution block must
permit exactly the outstanding authorised revisions, preserve every unrelated
head and run all DDL/version updates/postchecks in one external transaction.
Set lock timeout to 5 seconds and statement timeout to 30 seconds. Abort and roll
back if any data digest, identity ciphertext, original number/date/history,
unrelated constraint/index/FK or unrelated migration head changes unexpectedly.
The two intentional constraint changes above are the only permitted differences.

Until that schema is approved and applied, the application displays a clear
pending-003 message and disables new-registration Save. A forged POST also stops
before inserting a row or allocating a number. Existing profiles can still be
retrieved and edited while preserving their historical identity values.

## Verification

Focused tests use only local session-temporary PostgreSQL tables and synthetic
records, including account IDs 644/645. They cover new registration without ID or
posted email, authenticated email overrides, fresh visits, populated editing,
number/date/history preservation, byte-preserved historical identity, one-user
uniqueness, isolated clients/earnings, CSRF, suspension, failed commits, pre-migration
refusal and migration rollback. A headless HTTPS browser test submits the actual
simplified form and verifies Save, Edit, Cancel and reload behaviour.
No production migration or registration was executed. Folder Management remains
on HOLD. The educational franchise_license dependency remains unchanged.
