# Property & Owner delivery

Stage 1 uses existing bil_property records and manager_id authorization only.
Existing records/IDs are retained. Registration inserts only a property, and
address assignment updates only the selected manager-owned property's address.
No tenant, unit, lease, meter, account or owner record is created as a side effect.
No old wizard is invoked. Same-manager/name registration retries are serialized
with PostgreSQL transaction advisory locks and reuse existing records, including
archived records; other managers may use the same property name. This protection
applies to the new registration entry point, not historical maintenance scripts.

Existing scalar limits: property name 100 and property address 200 characters.
Overlong input is rejected, never truncated. Longer complete addresses require a
separately reviewed schema expansion. Archive status is displayed, not changed.

## Stages 2 and 3 blocked by ownership schema readiness

pm_owner_001 is unapproved for production and remains excluded from this deployment.
No owner queries or owner Save controls are activated in Stage 1. Owner assignments
are explicitly unknown/unavailable, not inferred from municipal account holders,
property managers or tenants. The complete property-and-owner Edit view is disabled.

The prepared architecture supports many-to-many ownership and reusable identities,
with independent property addresses. It does not yet store the requested owner
ID/registration number, residential/registered address, postal address or assignment
ownership percentage. BilProperty also has no property type column.

Before owner functionality can be enabled, review additive fields for identity
number, residential/registered address and postal address on owner identities;
percentage on each property-owner association (nullable, range 0..100); and a
property type field. Define aggregate percentage validation only where shares are
provided and require an explicit ownership-history transition for removal/reassignment.
Removal must append an ended/superseding association and must never delete the
owner identity or unrelated property associations. Changing shared identity details
must be a separate, clearly labelled action with manager-scoped authority, and
historical associations must retain their identity snapshots. Names must not be
used as automatic deduplication keys. Define accessible existing owners as explicitly
created or legitimately associated records, never a global searchable identity list.

Schema compatibility checks, verified backup, restore rehearsal and explicit approval
remain mandatory before production migration execution. The separate complete Edit
view follows only after the reviewed owner persistence is ready. This document is a
proposal/blocker report, not a migration or authorization to apply one.
