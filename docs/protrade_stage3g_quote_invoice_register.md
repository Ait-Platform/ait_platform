# Stage 3G: combined Quote / Invoice register

The ProTrade menu now has one Quote / Invoice entry, retaining explicit selected
shop context. Client Register row Quote buttons are unchanged. The new read-only
register allows shop selection and displays existing Carpentry BOQ quotations,
shop-linked mechanic quotations and existing mechanic invoice records. Open links
reuse the existing quotation/document screens and their actions.

The new queries validate the selected shop's owner and scope records by shop and
trade. Mechanic records additionally require an owned customer. Explicit shop
context on a mechanic document is validated and used for its shop profile.
Existing links without explicit context retain their prior behavior.

Legacy mechanic records with no shop_id are not assigned to an arbitrary shop.
Their existing register remains linked separately when such records exist.
Existing billed mechanic job cards without separate invoice rows appear as
legacy invoices, retaining their current document reference. No new invoice
numbers, inferred historical quotations or record conversions are created.

Existing source statuses and totals are displayed; accepted mechanic work is not
newly classified as billed merely because it is approved. Carpentry records show
Saved because acceptance statuses belong to Stage 3H. Viewing this page never
changes acceptance, work, billing or payment state.

Verification: 30 focused PostgreSQL-backed register, dashboard, profile, quote
and pricing checks passed, including browser checks. Tests cover owner/shop/trade
isolation, exact data preservation, legacy access, explicit document context,
existing mechanic actions and the unchanged Client Register quote flow.
git diff --check passes. No model/schema change or database migration.

Stage 3H acceptance and deliberate invoice generation remain deferred. Quotations
must retain their own record, and new invoices must receive separate numbers.
Stage 3I trade-specific work orders also remain deferred. No automatic invoice
conversion or new job-card/work-order functionality was implemented here.
