# ProTrade Output Email 022

The Email control was disabled for Price Lists when a shop had no debtors. Output
also used the basic PDF mailer rather than the existing cross-programme
`send_standard_report_email` document mode. The shared composer fields and Output
layout are retained; no new mail transport or PDF generator is introduced.

`protrade_output_email_022` follows `protrade_unified_prices_021` and explicitly
depends on `protrade_local_files_020` so a later application of the older
constraint cannot replace the final Price List guard. It permits a
NULL debtor only for the exact `price_list:all` Output selection, retaining the
existing historical-email exception and debtor requirements for quotations,
invoices and statements. It preserves all existing receipts, foreign keys,
unique request keys and financial records. It does not enable Folder Management. Verify whether prerequisite 020 is
already an ancestor of a recorded production head. If it is outstanding, obtain
explicit authorisation for it rather than executing it automatically. The DDL
also handles the earlier NOT NULL receipt layout safely.

Coordinate deployment with this migration. Without it, debtor-free Price List
sending fails closed before contacting the mail transport; ordinary linked
sends and opening/editing/cancelling the composer still work. No production
migration was executed as part of this change. Production execution requires
the verified identity/permissions, ancestry and backup checks in AGENT.md and
explicit authorisation. Preserve unrelated Alembic heads; do not upgrade all
heads or run startup maintenance.

Folder Management and Franchise remain on HOLD. PM's workflow is unchanged.
