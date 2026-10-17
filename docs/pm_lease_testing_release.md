# Lease particulars controlled testing release

Entry: Property Management > Lease > select an authorized property > Create Lease.

This release connects the five existing particulars sections to the existing lease draft JSONB store and adds authorized Review DOCX generation. The original Stage 5 master is packaged privately and verified against SHA-256 98b391c497cb2c70f931a6117a3b0074776256faaa8be91f3b6b9f4ad5d12937. All 37 placeholders are mapped; the generated document is marked DRAFT FOR REVIEW / NOT FOR SIGNATURE OR ISSUANCE. Optional blank signature fields render as writing lines, without invented signature dates. Additional clauses without an approved master slot block generation.

PM_LEASE_PARTICULARS_WRITES_APPROVED is an explicit Boolean environment setting defaulting to false. When separately activated it covers structured particulars, scoped draft creation and review generation. It does not enable the legacy PM_LEASE_DRAFT_WRITES_APPROVED flag, R2 uploads, lease activation or signature/issuance. No production setting is changed by committing this release.

Saving and generation retain manager authorization, property/draft locks, optimistic version checks and transactional commit-before-success. Saving one section preserves the others. Owner reads remain independent of owner-write approval. Generation snapshots retain merged values, master/output hashes, actor/time and prior history. Generated binary files are downloaded rather than archived to R2.

The administrator has supplied matching columns/defaults/lengths, validated constraints, seven indexes and pm_lease_001 history evidence. Do not rerun migrations. Original historical ownership migrations are included verbatim so existing Alembic history and PostgreSQL fixtures remain resolvable; no migration execution or stamp accompanies this release.

Focused local regression: 128 Python tests (lease, property and owner/portfolio suites), plus frontend property-selector and lessee-modal handler checks. Database tests use a new disposable loopback PostgreSQL cluster with synthetic temporary fixtures, not production or the existing development server. The isolated test cluster is stopped afterward. Historical viewer tests have 44 separately documented pre-existing fixture/environment failures; their security guards were not weakened to pass tests.

Reading, HOME, UIP, SACE, retirement and platform startup source are unchanged. Complete SACE/HOME workflows were not tested. Local diagnostics, backup tooling, AGENT edits, credentials, test outputs and unrelated worktree changes are excluded; exact files are listed in pm_lease_deployment_manifest.json.

Git delivery is separate from Render verification. An administrator must verify the deployed commit, startup/health, private master presence, production privileges and lease-specific flag before controlled live testing. Render availability and live workflow functionality are not asserted by local tests or successful push.
