# PM SOA presentation stages — 29 October 2026

## Ledger cleanup

Rent exposes only individual monthly Post actions; Assign is removed from the page. Manual Post Charge presents Recoverable Repairs, Late Payment Charges and Other Charge. Existing saved descriptions and legacy accepted posting values remain intact. Show More/Show Less use the actual displayed/total count, e.g. 5 of 20 and 20 of 20, with blue/amber buttons.

## Private SOA branding

SOA Branding is scoped to a selected property and its authenticated PM manager. Logo and letterhead choices are independent; selection never alters financial records. PNG/JPEG logos and single-page PDF/PNG/JPEG letterheads are accepted, capped at 5 MB with decoded image size limits. Uploaded originals and normalized PNG previews use the existing approved private R2 adapter under a manager/property branding prefix. Preview routes require the same property ownership checks and return private, no-store responses after SHA-256 verification. No public R2 URLs or new credentials are introduced. Simple letterheads use escaped business-name/address/contact text in private database metadata.

New independent Alembic revision: **pm_soa_brand_001**, branch **pm_soa_branding**, with no financial migration dependency. It creates only bil_pm_soa_branding and metadata/selection constraints. It was tested on isolated local PostgreSQL fixtures. Apply it using the established approved production migration procedure only after explicit approval. It has not been applied to production. Before migration, existing ledgers/statements remain usable without branding and the Branding hub shows the required revision. R2 configuration and approval checks remain mandatory for uploads/retrieval.

## Tenant-ready statements

Tenant Statement is linked from the tenant ledger and retains existing authenticated PM property/tenant access checks. It is a tenant-facing presentation/print view, not a new public-sharing endpoint or tenant-login system. The month defaults to the South African current month. Optional From/To values replace the respective month's boundary; both endpoints are inclusive.

Opening balance sums signed amounts before From; period rows use effective transaction dates; closing balance is opening plus period movements. Reversal/replacement entries and debit/credit references are retained. Notes, correction reasons, actor IDs, audit timestamps and internal linkage are excluded from the statement projection. An undated historical entry blocks the period calculation with a clear error instead of silently guessing its period. Account balances remain tenant-wide, consistent with the existing PM ledger; historical transactions are not moved between properties or tenants.

Selected private branding appears above transactions. Print CSS hides application navigation, filters and action controls, repeats table headings and preserves transaction rows. No statement-generation, viewing, filtering or printing action writes financial records. No additional statement migration, automatic posting activation or production test transaction is included. Production deployment, private R2 access and physical browser printing remain separate live checks.
