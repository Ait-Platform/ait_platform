# Municipal tariff publications ? Stage 3A

The existing Annual Municipal Tariff Register now links to **Download Municipal Tariffs**. Administrator-only GET lists retained evidence; a CSRF-protected POST checks official municipal document indexes for the selected financial year. No GET performs downloads. No tariff, verification or activation rows are written.

## Official discovery and evidence

Discovery checks the main municipal annual tariff index, electricity annual index and service tariff index. It follows actual PDF links; it does not guess publication filenames. Only HTTPS and the explicit eThekwini hosts in `municipal_tariff_sources.HOSTS` are allowed, including every redirect. TLS verification remains enabled. Network failures, HTML challenges, oversized/damaged/encrypted PDFs and financial-year ambiguity are reported rather than accepted as rates.

A municipal listing label is classified as DRAFT, PROPOSED, FINAL_LABELLED or UNCLASSIFIED. A final filename is not council-adoption evidence. All retained publications have approval status UNVERIFIED / HOLD. Listing date, HTTP Last-Modified and UTC retrieval date remain distinct from approval/adoption dates. PDF title-year discrepancies remain visible issues.

Retained evidence contains the original PDF bytes, municipal PDF and listing URLs, financial year, label, listing publication date when stated, retrieval timestamp, SHA-256 checksum, byte count, page count and unresolved issues. No rates are extracted in this stage. No candidate or active tariff values are replaced.

Each request checks three official indexes and retrieves at most three discovered publications (remaining links are displayed). Downloads have a 10-second socket timeout, shared 75-second retrieval budget, 2 MB HTML and 25 MB PDF limits. Failures preserve existing documents and tariffs; partial downloads never receive a successful receipt. These bounds do not change server timeouts.

## Existing storage infrastructure

Storage reuses the platform's immutable R2 transport when its existing R2 environment configuration is available. Keys are scoped below `municipal-tariffs/ethekwini/<year>/`: content-addressed `documents/<sha256>.pdf` and unique `receipts/<id>.json`. PDFs are written and read back before their receipt is published. Exact-byte repeats reuse the PDF; receipts preserve separate retrieval observations. Downloads verify the receipt, checksum and byte count again.

The shared asset bucket may be public: it receives only already-public official documents and public municipal metadata. Administrator identity is recorded in server logs, never in those public objects. No private uploads or user records are stored through this feature. The application evidence download endpoint requires administrator access.

An explicitly configured `PM_TARIFF_DOCUMENT_ROOT` can use an existing persistent disk instead. Static/upload directories and path traversal are rejected. Local development otherwise uses the private Flask instance directory. Render fails closed when neither existing R2 configuration nor an explicit persistent document root is available, rather than claiming ephemeral files are durable. No deployment settings or production storage were changed by this implementation. Operators must verify their existing durable backend before production use.

The latest 20 receipts are shown per year. Existing files are never overwritten or deleted. R2 listing overflow is reported explicitly.

## Verification

Automated tests use deterministic listing/PDF fixtures, isolated PostgreSQL test tables and temporary filesystem storage; no municipal website or production storage is used. They cover admin/CSRF enforcement, discovery/status/date/year handling, malformed sources and redirects, PDF integrity, storage immutability, no tariff writes, failure handling and the browser workflow.

Read-only live discovery on 10 November 2026 found:

- Main municipal 2026/27 tables: https://www.durban.gov.za/uploads/0000/13/2026/06/22/eth-final-2026-2027-tariff-tables.pdf ? listed 22 June 2026; 16,521,001 bytes; 289 pages; SHA-256 `59b640ec20d9a8a408ebd92a378dae66632470e703a7f3fed2509a5bf9c6a56e`.
- Separate electricity publication: https://electricity.durban.gov.za/uploads/0000/17/2026/10/06/tariffs-and-charges-2026-2027.pdf ? listed 6 October 2026.

The main PDF was fetched and validated without any production upload or database write. Discovery success is not adoption verification or proof of component applicability.

## Deferred stages

Extraction, component mapping/NEW tariffs and comparison belong to Stage 3B. Whole-schedule review/approval/activation belongs to Stage 3C. Annual scheduled checking and administrator notifications belong to Stage 3D. No working scheduler was identified in the inspected application paths; no scheduler or external service is introduced here. Existing individual activation is unchanged and cannot consume these source receipts automatically.
