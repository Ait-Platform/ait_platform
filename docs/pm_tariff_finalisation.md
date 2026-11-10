# Tariff management finalisation ? 10 November 2026 (SAST)

Scope: approval persistence and evidence-specific HOLD review only. No production tariff activation, database/configuration changes, migrations, METSOA changes, reading changes or PDF/email/Save Report changes.

## Approval audit persistence

Source approval and individual variance approvals/rejections retain actor ID, action, UTC timestamp, year, tariff code/reference, old/new evidence, document digest and scope in signed immutable records. Actual tariff activation continues to use the existing BilTariff and bil_tariff_verification transaction/audit. No new tariff database or correction workflow was added.

Storage now reuses existing PM infrastructure:

- A configured private PM_TARIFF_REVIEW_ROOT, or private-review beneath PM_TARIFF_DOCUMENT_ROOT. On Render, the path must resolve to a separate writable persistent filesystem mount. Image/root, overlay and memory filesystems are rejected. Paths beneath public/static/upload storage remain forbidden by the existing document adapter.
- If no disk root is explicitly configured, the existing approved private PM R2 transport (lease_r2, already reused by Utilities evidence). It uses the existing PM_LEASE_PRIVATE_STORAGE_APPROVED and PM_LEASE_R2_* configuration, with a separate private/pm-tariff-audit/ namespace. Shared/public Reading/HOME storage is never used. Existing bucket/credentials/policies were not changed; permissions for this prefix require live verification.

Explicitly configured disk storage is never silently switched to a different backend; this avoids hiding existing approvals. Local instance storage remains available outside Render. The existing immutable conditional R2 PUT/read-back is reused; different bytes cannot replace an existing audit key. After writing, policy verifies exact read-back, signature and scope before returning success. Idempotent retries verify existing evidence instead of trusting the presence of a filename. Failed configuration/write/read-back/integrity checks do not report successful approval.

Application restarts/deployments recreate the adapter over the same persistent objects/files. The signing key must remain available; Flask SECRET_KEY_FALLBACKS is supported for deliberate signing-key rotation without rewriting historical records. No audit retention/deletion/overwrite operation was introduced.

Live Render mount, private bucket credentials and prefix permissions were not inspected or modified. Durable private storage must be present/approved in the running configuration; an ephemeral path is not accepted as proof of persistence. Tests use temporary files and mocked private object storage, never production objects.

## HOLD findings from already-downloaded official evidence

The register now exposes these findings with source URLs, PDF pages and SHA-256 identifiers. Documented rates are evidence, not approval or new active versions. The findings manifest is a read-only review aid; it cannot activate tariffs.

| Item | Documented evidence | Remaining reason for HOLD |
|---|---|---|
| Official adoption | 2026/27 MTREF PDF19 submits resolutions for consideration/adjustment. PDF30 item1.2.12 refers to circulated tariffs, but contains R1.50/kL infrastructure surcharges. Tariff tables PDF288 contain R1.48/kL. | No binding Council decision/amended annexure linking the exact tariff table and resolving the conflict. A final filename/general adoption announcement is insufficient. No surcharge rate was changed. |
| Electricity | Official booklet PDF2/9/10: Scales3/4/8/9/13, 357.47c/kWh = R3.5747/kWh excluding VAT, effective1July2026; energy rate includes service charge. | Actual applicable municipal scale/account evidence and adoption remain unverified. Scale12/FBE, net billing, TOU and business rates cannot be inferred. The reviewed extractor now captures supported rate/unit/date facts; it requires existing verified scale evidence before clearing calculation mapping. |
| W_MGT / CCMGMT | Tables PDF272: monthly distribution charge payable per submeter, R30.50 excluding VAT. | This is not established as equivalent to the existing aggregate W_MGT property charge. Verify actual service and quantity/allocation basis; no assumed multiplication or calculator change. |
| W_LOSS | Tables PDF270: R21.60 for underground leaks for individual dwellings. MTREF PDF31 describes proposed fixed monthly domestic levy clarification. | Monthly wording is documented, but adopted policy and applicability per dwelling/customer/individual or bulk meter are not established. Existing per-meter calculations remain unchanged. |
| Refuse | Tables PDF262: domestic bags collected once weekly, billed per month/part; valuation exemption <=R400000; charged bands R62.53/R107.83/R139.04/R184.23/R246.98/R305.95 excluding VAT. | Exact valuation and service/billing category are unknown. AboveR350000 alone does not select a2026/27 bracket or prove non-exemption. An old matching amount is not classification evidence. |

All five overall items remain HOLD. The electricity rate/unit/date extraction gap is addressed, but this does not certify Almond's scale or legally adopted annual rates. No unsupported category or historical value was replaced. Historical water publication conflicts remain unaffected.

Official files used:

- https://www.durban.gov.za/uploads/0000/13/2026/06/22/eth-final-2026-2027-tariff-tables.pdf
- https://www.durban.gov.za/uploads/0000/11/2026/06/05/ethekwini-municipality-20262027-mtref.pdf
- https://electricity.durban.gov.za/uploads/0000/17/2026/10/06/tariffs-and-charges-2026-2027.pdf

The existing administrator tariff editor remains the correction mechanism. Missing adoption/category evidence cannot be cleared merely through a variance decision or source approval. Automatic activation remains disabled by default and was not enabled in production.
