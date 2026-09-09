# UIP pilot completion handoff

Implemented locally only. No deployment, production migration, real member email, real provider request, or production pilot credit allocation was performed. Production remains at its previously verified `uip_p10_finance`; this phase did not connect to it.

## Delivered and existing infrastructure reused

- Governance remains `UipSurvey`, its questions/results, `UipSurveyResponse`, the dated register eligibility function, unique `(survey_id, member_id)` response constraint and existing decision/finalization service. Invitation and authenticated votes call one ballot writer under a survey row lock.
- Reused the singleton `app.extensions.mail` / Flask-Mail and existing SMTP configuration. No new mail provider.
- Extended existing `CoreOrganizationWallet`, `CoreOrganizationLedger`, `CoreAiRequest`, and `CoreAiUsage`. These were scaffolding with no suitable gateway implementation. No parallel AI wallet/request/usage tables were created. Individual `AitTokenWallet` and payment-provider behaviour are untouched.
- Replaced the UIP Luna placeholder with an adapter to `app/services/ait_ai_gateway.py`. Other products' provider integrations are unchanged.
- UIP actions continue to use `UipAuditEvent`, never CoreAuditEvent. Reused existing UIP layout, eight collapsible sidebar sections, typography and pastel summary classes.

## Schema and migration

`uip_p10_finance` -> `uip_p11_completion` (frozen PostgreSQL DDL).

One new table: `uip_voting_invitation`. It contains the organisation/survey/member binding, unique digest, expiry, attempt count, timestamps and truthful mail status. Composite foreign keys enforce organisation consistency; unique survey/member prevents duplicate invitation entitlements.

Added nullable extension columns to the four existing shared AI/wallet tables: wallet status/update timestamp; ledger reference/type/product/actor/request/balance-after; AI request UUID/product/feature/input digest/credits/error category; usage credit charge and organisation-ledger link. Existing legacy columns and individual-wallet usage linkage remain in place.

Added survey eligibility snapshot and permitted a nullable actor for invitation responses and their survey audit event. Existing actor values are not changed. A database guard only permits an anonymous audit actor on a secure-invitation survey response event. Eligibility snapshots become immutable once set; new surveys freeze the dated eligible register at creation. Existing surveys freeze on first invitation send/response. Existing response eligibility evidence is retained.

Triggers make UIP credit ledger rows append-only, prevent a controlled wallet from becoming negative and preserve frozen survey snapshots. Legacy rows with no UIP product/status discriminator retain their previous semantics. No existing data is backfilled or rewritten. Downgrade refuses deletion of voting/credit history. Normal Alembic application is a future separately authorised operation.

## Email and voting

Managers/committee administrators open Governance -> Surveys -> survey -> Voting Invitations. GET only previews; a CSRF-protected POST with a signed, 15-minute confirmation sends up to 100 explicitly previewed recipients. The plan binds organisation, survey, actor, selected member and email digest. Changed recipients require a fresh preview. Bulk sending selects uninvited/failed recipients; individual resend rotates the token, preserves eligibility, and skips already-voted members. Missing/unusable email is visible.

Tokens use `secrets.token_urlsafe(32)` (256 random bits) and only SHA-256 digests persist. The private URL fragment is transferred into a CSRF-protected POST and removed from browser history before submission; it is not sent in the URL to web access logs. No login, account creation or general UIP access is granted. Poll status, expiry, organisation, survey and voter binding are checked for both display and submission. Repeated or simultaneous submissions yield one response. Public pages have no navigation to private UIP content, use no external scripts, and send no-store/no-referrer headers.

Each message has one recipient, organisation identity, poll title/purpose, close time, private link and intended-recipient wording. Attempt evidence is committed before mail delivery. States are ATTEMPTED, ACCEPTED, FAILED and TEST_SUPPRESSED. ACCEPTED only means accepted by configured SMTP; there is no delivered/read claim. SMTP debug must be off. A scoped logger filter suppresses UIP query/form payloads from the existing shared request trace; other products' trace behaviour is unchanged.

Audits include batch initiation, attempt, resend, accepted/test-suppressed/failure, vote received, existing survey finalization (closure), and decision creation. Public vote audits name the survey, not an identifiable response or token. Confidential poll choices remain subject to the existing aggregate-only UI; participation is visible to poll administrators. Confidential here is application-level privacy, not cryptographic anonymity from database administrators.

## Help and UI

`/uip/<org>/help` covers Command Centre, Residents & Properties, Operations, Service Providers, Governance, Finance, Reports and Administration. Task-oriented text includes calling a vote, sending invitations, work orders, Finance terminology, audit and AI credits. Top-bar Help chooses a context anchor centrally. Help is not a ninth sidebar heading. Administration includes AI & Wallet, and authorised staff have a top-bar AI assistant link.

## AI gateway and context

UIP -> scoped advisory adapter -> shared AIT gateway -> organisation credit reservation -> configured provider adapter -> usage metadata / UIP audit.

Initial tasks: communication/governance/minutes drafting, issue summary/category, aggregate issue activity and Finance explanation. No tools or record-writing capabilities are sent to the provider. Outputs are escaped text, returned for review, and never sent or applied automatically. Old AI entry points now open the review form rather than charging on an unreviewed placeholder action.

The operator explicitly approves minimal source text. Common credential/banking patterns are rejected, and emails/long phone-like numbers redacted. This is an additional safeguard, not a substitute for operator review. Database context is restricted to the current organisation: structured issue category/status/priority, aggregate issue counts or recorded Finance totals. No private document, contact list, password or bank data is automatically collected.

Provider/model/endpoint/key/rate are server configuration. Built-in `openai_compatible` uses the [Chat Completions protocol](https://developers.openai.com/api/reference/cli/resources/chat/subresources/completions), without hard-coding a model or adding a real credential. Other providers can register a server-side callable in `AIT_AI_ADAPTERS`; UIP code remains unchanged. `test` is usable only with Flask TESTING enabled. External calls have connection/read timeouts and no redirect following. Provider errors are converted to generic error categories; raw provider errors are not stored.

## Wallet, charging and permissions

One organisation wallet; no ratepayer AI wallets and no Finance transactions for credits. Platform allocation starts a new wallet at zero and records an auditable amount/reason/UUID. Existing legacy wallets require explicit platform adoption: an opening ledger adjustment reconciles the existing balance without discarding history. No Manor Gardens name, permanent free entitlement or commercial price is hard-coded.

Fixed positive configurable AIT credits per successful request; no invented rand/provider price. Unknown provider token counts and cost remain NULL. Provider token counts, when available, are metadata distinct from AIT credits.

The organisation/wallet lock serializes allocation/reservation. A durable AI_RESERVATION ledger debit precedes the provider call. A unique organisation/product/request UUID plus input digest prevents repeat dispatch and double debit, including concurrent retries. Success records linked usage atomically; known failure records a reversal/refund and zero charge. A pending request after a process interruption retains its reservation and is never automatically replayed. Platform operations must investigate the reference/provider outcome and, if appropriate, use an explicitly reasoned credit adjustment; no automatic timeout retry can cause duplicate spend. Draft text is deliberately not retained, so an idempotent replay reports prior status rather than replaying sensitive output.

Manager: view wallet. Manager/committee/receptionist: consume AI where feature roles allow. Issue/activity require manager/receptionist; complete Finance context requires manager. Platform allocation additionally requires the existing persisted `User.has_role('admin')` permission, not session flags. The allocating administrator must also have normal authorised UIP manager access for this organisation. Organisation managers without the global admin role cannot mint credits. Providers and members cannot administer wallets; provider-only accounts cannot browse governance or AI. All authenticated writes and public voting submissions use CSRF protection.

## Validation and acceptance

One consolidated focused run exercised 179 selected tests (pilot, existing Finance, work orders and governance). Initial result: 173 passed, 3 test-setup failures and 3 Windows temporary-directory setup errors. Targeted reruns corrected only those failures and checked final privacy/concurrency safeguards. Every selected check subsequently passed; no full historical suite was run.

Additional final checks passed: simultaneous different AI requests cannot overspend; simultaneous identical UUID cannot double-dispatch/charge; simultaneous public votes create one ballot; shared request trace redacts UIP payloads only. Python parsing, all UIP HTML template parsing and `git diff --check` passed.

The local end-to-end journey passed: controlled allocation, manager balance view, eligible poll, suppressed individual mail, no-login vote, duplicate rejection, participation/privacy, final results and formal decision, task Help, mock gateway draft, one debit and usage history, provider denial, empty-wallet AI denial with ordinary Finance/work orders still available, and cross-organisation denial. No real mail or API spend occurred.

Migration/model column/type/nullability/unique parity passed. Original-column snapshots across the synthetic existing local schema remained identical after Phase 11; the only new table was empty. The local harness rejects non-loopback hosts/database overrides and drops its isolated test schemas. `site_hit` and `visit_log` are excluded. This is local preservation evidence, not a fresh production preservation assertion.

Eight desktop/mobile screenshots (1440px/390px) cover Help, Wallet, Assistant and Invitations with no page horizontal overflow. Artifacts: `C:/Users/Sanjith/AppData/Local/Temp/uip-pilot-validation/`. Desktop Help and mobile Wallet were visually inspected.

## Required configuration / production readiness

- Existing Flask-Mail SMTP host/port/TLS-or-SSL/login/password/default sender must be valid. Keep SMTP debug off. Tests require suppressed mail; enable real sending only in an explicitly authorised environment.
- `UIP_PUBLIC_BASE_URL`: trusted HTTPS application origin, for example `https://your-host.example`; never derived from a request Host header.
- `AIT_AI_PROVIDER`: `openai_compatible`, or registered adapter name.
- `AIT_AI_MODEL`: server-selected model name (maximum 50 characters).
- `AIT_AI_ENDPOINT`: trusted HTTPS chat-completions endpoint for the compatible adapter.
- `AIT_AI_API_KEY`: server environment/config secret only.
- `AIT_AI_CREDITS_PER_REQUEST`: chosen positive integer AIT credit charge; no production rate was invented.
- Optional Flask config `AIT_AI_ADAPTERS`: mapping of provider name to trusted server-side callable accepting model/prompt/reference and returning text plus optional token counts.
- Existing strong Flask secret key, HTTPS and normal secure session/proxy configuration remain required.
- An authorised platform administrator and UIP manager membership are needed for controlled initial allocation. No pilot credits were allocated to production.

Before production: review/authorise Phase 11 migration and application deployment separately, supply the real mail/provider configuration and chosen credit rate, verify recipient data/eligibility and approve the pilot allocation. None of these production actions was performed. The full shared application factory was not started by this validation; it contains unrelated startup behaviours, so the existing isolated UIP harness was used.

## Files created/changed

- `app/models/core.py`
- `app/models/uip.py`
- `app/models/uip_governance.py`
- `app/models/uip_invitations.py`
- `app/services/ait_ai_gateway.py`
- `app/uip/__init__.py`
- `app/uip/completion_routes.py`
- `app/uip/gateway.py`
- `app/uip/log_privacy.py`
- `app/uip/operational_routes.py`
- `app/uip/pilot_routes.py`
- `app/uip/routes.py`
- `app/uip/services/ai.py`
- `app/uip/services/governance.py`
- `app/uip/services/invitations.py`
- `migrations/versions/uip_p11_completion.py`
- `templates/uip/ai_wallet.html`
- `templates/uip/assistant.html`
- `templates/uip/audit/list.html`
- `templates/uip/base.html`
- `templates/uip/help.html`
- `templates/uip/invitations.html`
- `templates/uip/public_vote.html`
- `templates/uip/ui.css`
- `tests/uip/conftest.py`
- `tests/uip/test_pilot.py`
- `docs/UIP_PILOT_COMPLETION.md` (this handoff)
