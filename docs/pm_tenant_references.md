# Tenant payment references

Tenant References use a random TR- prefix and 16 hexadecimal characters (19 characters total). They never encode tenant IDs, identity numbers, registration numbers or lease IDs.

The existing bil_tenant.notes text field stores a reserved line: [AIT Tenant Reference: TR-XXXXXXXXXXXXXXXX]. Existing free-form notes are preserved. Keep this line intact when changing notes. No schema change or migration is required.

Registration assigns the reference in the same transaction as the tenant and representative. Missing references for the current manager’s existing tenants are completed during the next authorised registration or explicit selection, never during a GET request. An allocation lock acquired before tenant row locks and collision check prevent reference reuse. Editing a lease or selecting the same tenant on renewal reuses the stored reference. Failed saves roll back reference assignment as well.

BilTenant.tenant_reference exposes the value for statements. tenant_reference.resolve(session, manager_id, reference) provides a read-only manager-scoped lookup for payment allocation and reconciliation. The rental ledger displays the reference and uses it as the default for new payment entries; historical transaction references and internal lease references remain untouched.

Signing forms carry an authenticated snapshot of selected party keys and saved Signing fields. A stale draft version may rebase only when those values have not changed. A real conflict preserves entered information and requires explicit confirmation against the latest saved values. Approval and signed-document locks remain mandatory.
