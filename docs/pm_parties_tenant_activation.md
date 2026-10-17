# Parties tenant register activation

The existing lease write gate is `PM_LEASE_PARTICULARS_WRITES_APPROVED` (Boolean, default false). An administrator must explicitly approve and set it to true in the Render web service environment. The shared `PM_LEASE_DRAFT_WRITES_APPROVED` remains unchanged. Code cannot establish the actual live environment value without authorized Render access.

## Additive prerequisite (not executed in production)

`docs/proposed_migrations/pm_tenant_001.py` is a new independent Alembic branch. It creates only `bil_tenant_identity`, its primary key, manager foreign key, three check constraints, and a manager/type/normalized-ID unique index. Model: `app/models/tenant_identity.py`.

Fields: stable integer id, manager_id, party_type, legal_name, identity_number, address, email, phone, signatory_name, signatory_identity_number, signatory_capacity, created_at. No changes to existing `bil_tenant`, sectional units, properties, owners, leases or ledger tables. Legacy `bil_tenant` requires a sectional unit and cannot safely store these reusable identity particulars as it stands. No existing records are copied, inferred or rewritten. The new identity id can be linked explicitly to future accounting functions; no debtor integration is implemented.

Before approving production execution: inspect existing tenant identity objects and privileges; obtain a verified current backup and restore rehearsal; review the additive migration and normalized-ID uniqueness policy. The prepared file is deliberately outside the active migration graph. After explicit schema approval, integrate this independent branch into Alembic and apply only `pm_tenant_001`, preserving all existing revision rows. No blind Alembic stamping. No startup schema creation is implemented. Downgrade refuses deletion if tenant identities exist.

## Workflow

Lease property selection -> Parties -> Add Tenant / View Tenant list -> Save Tenant -> select saved tenant -> select one owner representative -> Save particulars -> existing Review / Word generation. Tenant registration and lease saving are separate explicit transactions; draft party values are snapshots, so later registry changes never silently rewrite saved or generated leases. Duplicate tenant submissions for the same manager/type/normalized identifier reuse the existing identity without overwriting it. Other managers' tenants are not visible or selectable. Existing legacy draft schemas remain readable and generate through existing mappings.

Owner names, IDs, notices address and contacts come from associated owner records. No capacity is invented or confirmed: an existing capacity is preserved only for the same known representative identity. Selecting an owner does not assert legal authority. Existing signature and generation snapshots remain intact. Master DOCX unchanged.

## Verification

Three essential PostgreSQL integration checks cover the prepared migration in disposable storage, natural/juristic tenant registration, duplicate reuse, select/save/reopen, manager authorization, write-disabled and schema-missing failure, representative selection, preservation of other particulars, guarded rollback, and one complete 37-placeholder Word merge. A frontend check covers single-owner selection and removed duplicate panels. These do not prove production schema installation or Render operation.

## Production status

No production migration, environment change, database write or live email/notice/addendum action is authorized or performed by this work package. New tenant saving is unavailable until the required schema exists and the lease-specific flag has been explicitly approved. Existing drafts and Word generation remain subject to existing schema and write gates.
