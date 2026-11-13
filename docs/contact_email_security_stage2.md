# Contact form email security ? Stage 2

Only `/contact` and necessary quota/model/migration support are changed. Successful legitimate submissions keep their existing configured recipient, submitted Reply-To, contact-message format, success flash and redirect. User-supplied recipient fields are ignored. GET/HEAD continue to display the contact form without using quota. CSRF protection remains in place. Basic server-side validation rejects missing/invalid fields, oversized content and subject/name header injection without sending.

## Shared rolling-hour limit

PostgreSQL reserves each allowed email attempt before SMTP. A per-IP transaction advisory lock serializes count-and-insert operations across workers/instances; the database clock and timestamp index enforce at most three attempts in any rolling 3600-second window. Changing browser sessions or workers does not reset the quota. Exceeding it returns plain-text HTTP 429 with Retry-After and Cache-Control: no-store. Different IP addresses have independent quotas; visitors sharing a public/NAT IP share the quota.

All reserved attempts count for an hour, including missing mail configuration, suppressed delivery, SMTP failure or uncertain transport outcomes. There is exactly one transport call per allowed POST and no automatic retry, fallback or quota refund. A storage/commit failure prevents sending. Failure responses contain controlled text, and these new error logs record only the exception class rather than SMTP error details. There is no new job or retry scheduler.

Quota storage contains only a random attempt ID, HMAC-derived IP key and timestamp. It does not retain a plaintext IP, recipient or message. The HMAC uses the application's existing SECRET_KEY with a contact-specific purpose prefix. All workers must use the same stable application secret; rotating it changes quota keys. Expired entries stop counting automatically; no automatic deletion/retention job was introduced.

## Client IP trust boundary

Render's runtime `RENDER=true` flag selects the public Render/Cloudflare ingress path. Only `CF-Connecting-IP` is used there; missing, malformed, multi-value or scoped addresses fail closed with a controlled 503. Arbitrary X-Forwarded-For prefixes are never used for quota identity. Outside Render, only the socket remote address is used and forwarded headers are ignored. IPv6 representations and IPv4-mapped IPv6 addresses are normalized. This change is local to contact and does not alter global ProxyFix or other workflows.

This assumes requests on Render reach the application through its public Cloudflare ingress. Direct/private access must not be treated as authenticated edge traffic merely because it supplies a header. Live verification must confirm that custom-domain and onrender.com traffic both supply the expected edge header and identify the visitor correctly. Additional self-hosted proxies would need their own explicitly trusted topology; they are not automatically trusted here.

References: [Render public ingress](https://render.com/articles/how-render-handles-ddos-attacks), [Render runtime flag](https://render.com/docs/environment-variables), [Cloudflare client-IP header](https://developers.cloudflare.com/fundamentals/reference/http-headers/).

## Required production migration

Apply the independent `contact_email_security_001` revision before expecting contact POSTs to send. It creates only `contact_email_attempt` and its IP/timestamp index. It has no dependency on the LOSS or ProTrade branches. Missing tracking storage prevents SMTP; contact GET remains available. Destructive downgrade is disabled to avoid resetting active quotas accidentally.

Using the Render service's assigned database configuration, independently verify current_database() and current_user first, then run:

```sh
python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.upgrade(c,'contact_email_security_001')"
```

Verify `SELECT to_regclass('contact_email_attempt');` and the new Alembic revision. Use this targeted migration rather than upgrading unrelated branches. It succeeded against verified local ait_local_db. Before/after snapshots confirmed existing LOSS runs/results/responses/send records and ProTrade debtor/quotation/invoice rows were unchanged. Production execution and actual SMTP/ingress behavior have not been verified.

## Validation

48 focused tests passed: 24 contact PostgreSQL tests, 22 existing LOSS PostgreSQL tests, the removed-debug-mail-route regression and the Flask-Mail configuration regression. Contact tests execute the actual migration, quota helper and route against isolated PostgreSQL tables, with SMTP mocked. Coverage includes the three-attempt limit, rolling-window expiry, distinct IPs, restart/worker persistence, eight simultaneous requests allowing exactly three sends, durable commit before SMTP, failed/uncertain SMTP accounting, configuration and suppression failures, missing tracking table/commit failures, GET/HEAD, validation, CSRF, forwarded-header spoofing, Render edge-header selection/failure behavior, canonical IPs and hashed storage. No live email was sent.

No LOSS or ProTrade source, credentials, mail configuration or existing contact template was changed. Stage 1's reported production migration completion is separate from live sending/access-control verification, which remains outstanding.
