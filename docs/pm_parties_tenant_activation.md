# Tenant and representative completion

## Runtime

The two temporary owner/structured-lease environment gates have been removed from runtime code and configuration. Neither is needed in Render. No replacement switch was introduced. Existing login, active-user checks on lease routes, manager/property authorization, validation, schema checks, ownership-history triggers, optimistic draft versions and transactional commits remain. Separate legacy upload/private-storage gates remain unchanged.

The single main register is now `bil_tenant`: name, party_type, identity_number, address, email, phone and manager_id. Registration reads/writes explicit columns and never assumes property_id exists. It supports the production sectional_unit_id variant and the local property_id variant. Standalone registration does not create a unit. METSOA fields, foreign keys and existing billing code remain. The three new ORM fields are deferred, so existing billing queries do not start selecting missing columns before SQL rollout; the new register itself remains schema-gated.

`bil_tenant_identity` now represents a tenant's representative via tenant_id. It reuses legal_name, identity_number, phone and email. Natural persons have no representative row. Juristic tenants require a representative's name, ID, telephone and email, saved atomically with their tenant. No capacity input is required or invented. Old signatory/identity columns are retained nullable and obsolete check requirements removed, rather than dropping records or columns. The existing-table constraint permits one current representative per tenant.

Selected tenant_id and representative_id, identities and contact details are snapshotted in lease draft JSON. Historical snapshots and generated-document values remain unchanged. Old tenant_identity_id values are not guessed to be new tenant IDs. Old drafts remain readable/generatable; select a registered bil_tenant when resaving Parties. Master, merge tokens and Parts B-E remain unchanged.

## Required production SQL (not executed)

1. Obtain a current verified backup and successful isolated restoration. Independently verify the authorized production connection.
2. Run `docs/sql/pm_tenant_002_preflight.sql` in pgAdmin. Compare actual columns, types, constraints, indexes and privileges with the prepared SQL. Confirm the administrator-reported zero rows in both tenant tables. If either table contains records, stop for a data-preserving conversion plan; the SQL deliberately refuses to repurpose populated tables.
3. Review/approve `docs/sql/pm_tenant_002.pgadmin.sql`. Only after approval, execute the complete transaction. It adds three identity fields to bil_tenant, a manager foreign key, required-field check and normalized-ID unique index; allows NULL sectional_unit_id only where that column exists; adds tenant_id FK and unique representative assignment; removes obsolete representative-table tenant/signatory constraints and relaxes obsolete columns. It creates no tables, changes no business records, and touches no operational lease/document tables.
4. The already-installed pm_tenant_001 has been added unchanged to the active Alembic graph. DO NOT run it again. The new pm_tenant_002 is its child. The manual transaction advances only that branch's version row from pm_tenant_001 to pm_tenant_002 after DDL success. All other branch rows remain unchanged. Do not insert both ancestor and descendant heads or blindly stamp. If using Alembic instead of manual SQL, target pm_tenant_002 only and let Alembic maintain its branch state. Never execute both methods.
5. Run `docs/sql/pm_tenant_002_verify.sql` and verify the actual application's effective role has tenant/representative SELECT/INSERT plus sequence USAGE. Migration execution needs table-owner privileges. No automatic grants or role changes are performed.
6. Verify Render has deployed the new code, then test the complete workflow with authorized real users. No additional environment flag is required.

## Safeguards and limits

No production SQL was executed by this work package. Production schema compatibility is not inferred from local tests: the production column/constraint inventory still must be compared with the preflight output. Existing table definitions are not overwritten; collisions and unexpected foundations cause the transaction to fail. A rollback on an error preserves both schema and Alembic branch state. An automatic downgrade is deliberately refused once this table's meaning is changed; restore/reversal needs a separate reviewed plan.

Essential local tests cover both tenant schema variants, natural/juristic representation and visibility, atomic rollback, owner saving without switches, tenant selection and duplicate reuse, lease save/reopen and other-section preservation, unauthorized requests, and Word generation for both party types with no stale representative data. These tests use disposable PostgreSQL; they do not establish Render operation or end-to-end METSOA/SACE/HOME/UIP verification.
