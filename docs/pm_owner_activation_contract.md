# Owner Save activation contract

Current UI: Save Owner is disabled. Owner registration is not yet available.
The postal-same checkbox copies the residential/registered address and keeps postal
synchronized while checked. Unchecking permits independent postal input. New modal
entries and multiple-owner previews remain browser-only and are not persisted.

Activation is blocked until both ownership revisions have been explicitly approved,
applied and independently verified. Neither a button-label change nor a session preview
constitutes schema readiness. No owner persistence service is activated by this commit.

When approved, Save Owner must perform one database transaction:
1. Lock/revalidate the selected property's current manager authorization.
2. Select an accessible existing owner identity, or create the requested individual,
   company or trust identity with the approved deduplication rules. Never infer ownership
   from the manager, tenant or municipal account holder.
3. Insert the property-owner association and optional percentage, relying on database
   authorization/history guards and preserving other owner assignments.
4. Commit identity and association together. Any failure rolls back both; never display
   success for a partial or failed save. Shared-identity edits remain distinct from
   property-assignment changes.
5. Read and return the persisted, manager-authorized owner list for that property, then
   refresh its Owners cell immediately from that result. Preview entries must not be
   used as evidence of successful persistence. Multiple owners remain supported.

Production-readiness review remains blocked: the prepared read-only preflight reported
no usable explicitly supplied remote PostgreSQL DATABASE_URL. Render is not installed/
connected, and no authenticated browser session is available. Production schema,
Alembic state, privileges, record conflicts, backups and restored-database rehearsal
have not been verified. Obtain authoritative Render resource access before recommending
migration approval. The Stage 2 preparation branch must remain excluded from UI pushes.

After access is available: run read-only preflight on the authoritative target, inspect
records/constraints and the bil_muni_account.property_id gap, create and verify a backup,
restore it to a separate rehearsal database, and rehearse upgrade to pm_owner_002 using
the actual Alembic runner. Only after reviewing those results and obtaining explicit
approval may production execution occur. No production migration or backup was run here.
