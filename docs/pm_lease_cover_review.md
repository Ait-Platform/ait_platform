# Cover refinement and document asset findings

## Completed cover correction

Known individual contracting names use first initial and surname, with simple lower/uppercase surnames normalized for display: sunjithlall nanhoo becomes S. Nanhoo. All contracting parties are included. Companies, NPCs, trusts and unknown entity types retain their saved legal names. Cover-only abbreviations never change legal identities or saved snapshots. IDs, registration numbers, addresses, capacities and Landlord/Tenant labels are absent from the generated cover. Full legal identities remain in the Certificate. The Certificate begins after an explicit Word page break. Existing draft/approval restrictions remain.

Read-only local project search scanned 124 Word/PDF files (excluding backups, environments and dependency folders). The existing lease candidate app/static/pdf/pm/Lease Graham Curtis Clack.docx.pdf identifies 19 Dale Road on pages 2 and 25. Its first page was inspected visually: upper-right agreement heading and centered names joined by an ampersand, no party-role labels. The generated cover follows that simple arrangement; it does not insert the Dale document into the generated agreement or change its source file.

## Core Terms boundary - still unresolved

No standalone AIT Core Terms source asset or approved source-section boundary manifest was found. The current builder pins the verified property R01 reference and prepends the working Cover/Certificate, then retains the complete source body, including original cover/schedules/signatures. The existing Core Terms PDF renderer similarly renders that full development reference. Neither the R01 DOCX nor its PDF is asserted to be a separate operative-Core asset.

No pages or clauses have been extracted, rewritten, excluded or reclassified in this change. The requested five-part assembly is not complete. An explicit, reviewed Core Terms boundary/asset is required before arranging Core Terms, selected property conditions and a separate signing page without duplicate source sections or omitted legal content. Property-specific condition selection remains a separate outstanding persistence/assembly task; no rules for Almond/Dale were invented.

## Banking migration review

pm_lease_payments_001 creates only bil_property_payment_account and bil_property_utility_arrangement, their primary/foreign/unique keys and checks. It has no ALTER/UPDATE/DELETE/backfill, and does not change existing revision files or existing business rows. Its independent Alembic branch preserves existing heads; it checks existing foundation tables and refuses new-object collisions. Destructive downgrade refuses to run. Production preflight, backup/rehearsal and explicit execution approval are still required. No production migration was applied.

## Essential check and scope

One focused local PostgreSQL check passed: requested name example, multiple contracting parties, unchanged company/NPC names, cover privacy, full Certificate identities, explicit page separation, unchanged source clause, and static additive-only migration inspection. No full suite run. Source documents, existing particulars, Email/Print implementations, legal approval flags and unrelated programmes are unchanged. Only the cover builder, relevant cover expectation, essential test and this report are included in the cover commit.
