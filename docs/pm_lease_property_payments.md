# Reusable property banking and utilities

## Scope

Banking Details replaces Special Conditions in the eight-item Lease submenu. Property-level bank identities reuse bil_bank_detail; new links allocate accounts to Rental, Utilities or both. Existing property bank_detail_id and municipal/tenant bank relationships are unchanged. Exact matching current-property linked bank details can be reused; no global account lookup or cross-manager identity reuse is attempted. Duplicate property/account submissions are serialized under the existing property lock and do not add another identity. Bank identity rows are never edited or deleted by this feature.

Utility arrangements are versioned at property level. The user may explicitly reuse the exact utility text/references from a saved draft, enter a property-specific arrangement, or select No utilities are charged to the tenant (including Dale). There is no hardcoded property-name rule or automatic rewrite of Almond data. Existing utility tables, schedules, municipal accounts and historical records remain unchanged. Reuse copies only the explicitly saved utility particulars; historical source tables/images are not interpreted or imported automatically.

An explicit lease selection snapshots the chosen Rental/Utilities accounts and the selected utility version into existing items._banking_utilities. No property update automatically changes an existing draft or signed copy. Historical confirmation flags are preserved where present but no verification assertion is made or required. No bank verification service or legal approval is introduced. Lease Certificate mappings use the same saved banking/utility tokens, with purpose-labelled accounts. Other particulars and Special Conditions remain in JSONB.

A recorded original signed lease blocks payment/utility snapshot changes on both the new route and legacy banking save route. Use existing Addendum, linked to the exact signed original. A property utility version can prefill its exact wording into Addendum for user review; nothing is automatically generated, issued or sent. The old Special Conditions page redirects to Addendum; a POST to that old editing route is rejected without losing saved text. The underlying Word master/Core Terms, View Lease, Email and Print implementations are untouched.

## Additive migration - not executed in production

pm_lease_payments_001 is an independent Alembic branch (down_revision=None, branch_label pm_lease_payments), matching the existing Lease independent-branch convention. It requires existing bil_property, user and bil_bank_detail columns and refuses name collisions.

Creates only:

- bil_property_payment_account: UUID id PK; property_id FK RESTRICT; bank_detail_id FK RESTRICT; for_rental/for_utilities booleans; created_by FK user RESTRICT; created_at timestamptz default now. Unique(property_id,bank_detail_id); check at least one allocation. The unique index supports property lookup.
- bil_property_utility_arrangement: UUID id PK; property_id FK RESTRICT; positive version; details JSONB object; created_by FK user RESTRICT; created_at timestamptz default now. Unique(property_id,version). Previous versions are retained; routes insert new versions rather than updating them.

No ALTER, UPDATE, DELETE or backfill of existing business tables. No changes to existing revision files or heads. Readiness checks require both tables, foundation bank fields and the exact migration marker. Missing prerequisites render a disabled Banking Details page with the actual reason and reject writes. Existing saved details remain visible. No dummy successful save or automatic schema execution.

Before production execution: read-only inspection of actual foundation columns/types/keys, privileges, object collisions and Alembic graph; current verified backup; isolated restore and targeted rehearsal; separate explicit migration approval. Only then select a targeted pm_lease_payments_001 execution command after reviewing the installed migration graph and a maintenance entry point that does not invoke unrelated startup writes. Do not run this command merely as part of code deployment, and do not use a generic upgrade-head/all-heads command. No production command was executed here. Destructive downgrade deliberately refuses; application rollback preserves links, utility versions and existing bank data. Disabling/reverting the UI is the non-destructive operational rollback.

## Essential checks

Two focused PostgreSQL tests passed on a fresh disposable loopback ait_local_db, with temporary property, bank, lease and user fixtures. Migration applied locally only. Existing Alembic entries preserved. Checks cover multiple purpose allocations, retry duplicate prevention, exact existing utility text/references, explicit snapshot and Certificate mapping, version preservation, no-charge arrangement, original property/lease data preservation, signed-original protection including the legacy endpoint, Addendum prefill and History rendering, manager authorization, and safe missing-schema rejection. No full test suite run.

No production schema or data modified. No unrelated programme, master, signed document, mailer or viewer changes. New banking/utility registry functionality remains unavailable in production until the separately approved migration is applied; no readiness flag was bypassed.
