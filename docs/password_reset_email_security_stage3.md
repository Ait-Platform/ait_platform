# Password-reset email security ? Stage 3

Only `/forgot` and its necessary reset-specific support, models and migration are changed. The `/reset/<token>` handler, `app/utils/password_reset.py`, reset form and reset email template remain unchanged. Tokens retain the existing pw-reset salt, uid payload and 3600-second expiry.

## Request limits and public responses

Every CSRF-valid POST reserves one PostgreSQL IP slot before any account-dependent work. Unknown, empty, malformed and cooling-down addresses all consume slots. A per-IP transaction advisory lock and the database clock enforce at most three requests in any rolling hour across workers/instances. The fourth request returns HTTP 429 with the same generic message and an IP-derived Retry-After. Other accepted requests redirect to login with the identical generic flash, regardless of account existence, cooldown, delivery outcome or internal failure. No address or account status is returned. Missing IP or storage prevents sending while retaining the generic response.

All account lookup, cooldown decisions, template rendering and SMTP delivery occur in a background app context, so the HTTP response does not wait on SMTP for registered accounts. This avoids the large synchronous delivery timing difference; it is not a claim of mathematically constant response time. Every accepted request schedules the same type of task, including unknown addresses.

The worker resolves the account using its saved email, failing closed for case-ambiguous legacy accounts. Submitted recipient overrides are ignored. An account-level PostgreSQL advisory lock protects a durable reservation committed before generating/sending the reset email. The same account cannot trigger another send for 600 seconds, even from another IP, browser or worker. Account cooldown is never exposed through a separate response or header.

## Delivery and failures

There is one transport attempt per eligible reservation. Missing storage or reservation commit failure prevents sending. SMTP failure, suppression, uncertain delivery or outcome-recording failure does not refund the IP slot or account cooldown. No automatic retry or fallback is introduced. A later explicit request can send again after the account cooldown, subject to the IP limit. Attempt IDs prevent an old task's status update from overwriting a later reservation.

The background task is best effort, not a durable mail queue. Worker shutdown can interrupt or lose a task; no automatic replay occurs. An existing account reservation still enforces its cooldown, and the requester can submit again later within the limits. Actual transport acceptance does not establish inbox delivery. Tokens and reset URLs are passed only to the original reset template/shared sender; new request/delivery errors log exception classes without addresses, token values or SMTP error text.

## IP handling and storage

Reset has independent IP and account tables; it neither uses nor modifies Contact/LOSS quotas. It follows the already established public Render/Cloudflare trust boundary: Render runtime flag `RENDER=true` selects the single-address CF-Connecting-IP header; otherwise socket remote_addr is used. X-Forwarded-For is never used to choose the key. Missing/malformed/scoped addresses prevent scheduling. Canonical IPv6 and IPv4-mapped IPv6 forms share a key.

This assumes Render public ingress, not arbitrary direct/private traffic with user-supplied headers. Live verification must confirm correct client identity for both custom-domain and onrender.com requests. The implementation does not change global ProxyFix. IPs are stored only as a purpose-separated HMAC using the existing SECRET_KEY. All workers must share that stable secret. IP records contain no email or plaintext IP; account records store user ID, attempt ID, timestamp and outcome. Expired IP records stop counting automatically; no retention job was introduced.

## Required production migration

Apply `reset_email_security_001` using the running Render service's assigned PostgreSQL configuration, after independently verifying current_database() and current_user. It is an independent reset-only branch creating `password_reset_request`, its IP/time index, and `password_reset_send`. It does not alter users, passwords, reset tokens or other workflow tables. Missing tables prevent sending. Destructive downgrade is disabled to preserve active restrictions.

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'reset_email_security_001')"
```

Verify the new Alembic revision and `SELECT to_regclass('password_reset_request'), to_regclass('password_reset_send');`. Apply the targeted revision rather than upgrading unrelated branches. Local migration succeeded against verified ait_local_db. Before/after database fingerprints confirmed existing user, Contact, LOSS and ProTrade records were unchanged. Production execution and live delivery remain unverified. Stage 2's migration is reported complete by the user, while its live IP/delivery checks remain outstanding.

## Validation

77 focused tests passed: 29 reset PostgreSQL tests, 24 existing Contact PostgreSQL tests, 22 existing LOSS PostgreSQL tests and two adjacent mail regressions. The new tests execute actual migration, route, helpers, original token functions and original email template with SMTP mocked. Coverage includes generic registered/unknown/malformed/cooldown responses, three requests per rolling hour, separate IPs/accounts, restart/worker persistence, eight concurrent requests allowing exactly three IP reservations, concurrent account delivery allowing one send, response completion while actual background SMTP is blocked, 10-minute cooldown expiry, old token compatibility/expiry, stored recipients, SMTP/template/suppression/task-start failures, database failures before/after sending, CSRF, edge-header and IP canonicalization, source-user preservation and hashed storage. No live email was sent.

All other auth functions were checked unchanged; Contact, LOSS and ProTrade source, credentials and mail configuration were not modified.
