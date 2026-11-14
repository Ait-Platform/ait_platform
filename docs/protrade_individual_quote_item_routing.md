# Individual quotation item routing correction

## Inline quotation register (current)

The quotation register now expands saved items directly below the selected summary row. Only one row is expanded. Register Edit and quotation-number links use `/mechanic/shop/<shop_id>/debtor/<debtor_id>/quote?area=register&expanded_quote_id=<quote_id>`. The server validates the expanded ID against that debtor/shop/owner history; JavaScript toggles rows without leaving the register, with ordinary links as a fallback.

The working individual item form, stable IDs, locking, validation and calculations are unchanged. Save Changes, Cancel and Back return to the same expanded register. `/quotes/<quote_id>/items` remains compatible and redirects to the expanded register when no individual item_id is supplied. Item forms still post to that existing handler. The saved-item table is shared presentation markup rather than a second editor.

Real local HTTP/Chromium coverage verifies Q-0006 expansion below its row, switching between Q-0005/Q-0006 with only one visible, item editing, Save/Cancel return, reload persistence and immediate updated item/summary totals. Tokens, quotation count/identity, other items and accounting/email records remain unchanged. No migration is required.

## Traced path before the correction

`GET /mechanic/shop/6/debtor/15/quote?area=quote&quote_id=6`

1. `debtor_quote` verifies the shop/debtor and calls `register_trade_quote`.
2. Carpentry calls `carpentry_quote_form`.
3. A supplied `quote_id` sets the initial working area to `quote`.
4. `carpentry_quote.html` renders the whole editor. `carpentry_quote_hub.js` shows that area and `carpentry_quote_builder.js` builds every line. The whole form posts to `debtor_quote`.
5. The prominent Register Edit action still generated this URL; the separate item page was reachable only by the quotation-number link. This explains the reported live screen without assuming a failed migration or deployment.

## Previous item-register correction

Register Edit and the quotation-number link both open the selected quotation item register. Old bookmarked GET URLs containing quote_id now redirect there after the existing ownership checks. Adding item_id to an old URL directs it to that specific item.

Canonical register: `/mechanic/shop/6/debtor/15/quotes/6/items`

Canonical individual editor: the same URL with `?item_id=<quotation-scoped UUID>`.

The existing `carpentry_quote_items` handler and `carpentry_quote_items.html` are reused. Each row submits a specific item_id, not its array position. Existing JSON snapshots have no separate item table: their stable ID derives from the quotation identity and saved source-item identity. The selected item's ID is stored as quote_item_id on its next explicit save. GET never changes historical data; unedited items remain untouched. Old positional GET bookmarks resolve to canonical IDs; saves require the ID. Reordering does not redirect an edit to a different item.

The form explicitly posts to the item handler, shows only the selected saved item, includes its code in the heading and displays its saved line total. It retains the stale-edit check, debtor/quote locks and exact decimal arithmetic. Save copies the current snapshot and replaces only the matching item, recalculates the grand total and redirects to the item register. No new quote, numbering change, invoice, token deduction or email action is invoked. The explicit existing whole-editor route remains available for its existing workflows; individual Edit never routes through it.

## Verification and limits

PostgreSQL/Chromium tests cover both Register links, old direct navigation, explicit item-ID navigation, original saved values, stale submissions, reordering, Cancel, access isolation, same-record saving and reopening. A real loopback HTTP server serves the registered production route bodies, program templates and static JavaScript with an authenticated browser session and temporary local PostgreSQL fixtures. It avoids the unrelated database maintenance in create_app; its minimal test layout does not verify the global production theme.

The HTTP test uses the exact shop 6/debtor 15/quote 6 URL with Tomas/Q-0006 fixtures. CAR-003 changes from quantity 1 to 2, R259 + R65 gives R648; CAR-005 remains R825; the immediate register shows R1473. Wallet balance and transaction history, invoice/email tables, Price List records, quotation count and identity remain unchanged. Mail/SMTP calls are trapped. Browser screenshots of the selected-item screen and updated register are inspected locally.

No schema migration is required. Production records were not modified in testing; Render deployment and authenticated live verification remain separate.
