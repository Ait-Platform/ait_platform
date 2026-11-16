# AIT Franchisee Register release

## Delivered scope

AIT Admin Hub → **Franchisee Register** offers email invitations, registration status, private review, approval/rejection and explicit resend for unsubmitted invitations. The short form captures the requested name, SA ID, cellphone, email, residential address and optional occupation. Submission date is automatic; UI dates use South African time.

States are Invited → Submitted → Approved or Rejected. Only approval assigns `FR-0001`, `FR-0002`, etc., using a PostgreSQL sequence and a unique constraint. Row locks prevent replayed submissions/approvals. Numbers increase uniquely; rolled-back approvals can leave gaps. Rejected records remain in the audit history and do not receive a number.

The SACE lifecycle supplied the invitation pattern: cryptographically random tokens, stored hashes, expiry, single redemption and locked state transitions. Its programme-specific engagement/provisioning tables and access-grant functions are deliberately not reused for franchisee identity. Shared AIT users, Admin checks, CSRF, Flask-Mail transport and request-log privacy components are reused.

An invitation matches the email to an existing AIT user, case-insensitively. Happiness must be invited using her existing AIT account email. No `User` is created, overwritten or granted roles. People with no existing account can submit a registration with no linked account; approval does not create an account or programme entitlement. Ambiguous or duplicate account matches require review rather than automatic merging. Shops, debtors, documents, tokens, earnings and payment arrangements are not modified.

## Security and configuration

- Admin-only list/review/decisions. Invitations expire after 48 hours and are consumed on successful submission. Resending revokes the prior link. Required fields and SA ID format/date/checksum are validated server-side; this is not governmental identity verification.
- Unique email, linked-user and keyed identity-digest constraints prevent duplicate registrations. Invalid submissions do not consume a valid invitation. Duplicate conflicts return a generic message without disclosing another person's details.
- SA IDs and addresses are encrypted with the existing `cryptography` dependency. Their encryption key is required; there is no development-key fallback. Duplicate identity checking uses a keyed HMAC, not a plain enumerable ID hash. Lists omit IDs/addresses; only the private Admin review decrypts them.
- Responses are no-store/no-referrer/noindex. Sensitive screens block scripts, third-party resources and framing through CSP; locally styled forms work without JavaScript. Existing application log filtering omits franchisee contents and exception messages. Site-hit/visit records store the route pattern instead of the invitation token. Keep transport/proxy log access restricted; invitation links are bearer credentials.
- Registration explicitly does not establish a franchise contract or change prices, earnings, token charges, fees or programme permissions. No ID scans or bank details are collected.

Set these **Render secret environment settings before invitations**:

1. `FRANCHISEE_PUBLIC_ORIGIN=https://ait.mathwithhands.com` (canonical HTTPS origin, no trailing path). Invitation links do not trust request Host headers.
2. `FRANCHISEE_PII_KEY`: generate one Fernet key in an approved private environment:

   ```sh
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

   Store the result directly as a Render secret. Do not commit or paste it into chat. Retain it with secure operational backups. Do not regenerate it on deploy or rotate/delete it without a reviewed re-encryption and HMAC-index migration plan; existing records depend on it.

SMTP uses the existing AIT configuration. Delivery exceptions are handled without logging recipients or tokens. Delivery failures leave an Invited record for Admin-controlled resend. Actual inbox delivery remains a live verification step.

## Migration and exact deployment order

New revision: **`ait_franchisee_001`**, parent **`protrade_trade_codes_019`**. It adds only `ait_franchisee_registration` and `ait_franchisee_number_seq`; it does not change existing user or financial tables.

**No production migration was executed during development.**

AIT currently has startup auto-upgrade behavior. This new migration explicitly rejects contexts without `franchisee_registration_approved=True`, so startup cannot apply it silently. An unmigrated register responds with 503; existing programmes can continue. Keep the existing `SKIP_AUTO_MIGRATE=1` deployment control in place for controlled releases. Unrelated startup maintenance remains parked; no memory or renderer settings were changed.

Production procedure, only after migration approval:

1. Make the release commit available in Render's approved release/pre-deploy environment. Preserve the current authorised production `DATABASE_URL`; do not substitute local `.env` or obtain credentials from scripts/history.
2. Configure the two secret settings above. Verify the current production Alembic revisions read-only; confirm `protrade_trade_codes_019` is already applied:

   ```sh
   python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); command.current(c)"
   ```

3. Once explicitly authorised, apply only the new target using the explicit approval attribute:

   ```sh
   python -c "from alembic.config import Config; from alembic import command; c=Config('migrations/alembic.ini'); c.set_main_option('script_location','migrations'); c.attributes['franchisee_registration_approved']=True; command.upgrade(c,'ait_franchisee_001'); command.current(c)"
   ```

   Order: **019 → ait_franchisee_001**. Do not use `upgrade heads`, stamp past revisions, or call the application factory as a migration substitute. The existing Alembic environment loads metadata without application startup. The approval flag is required for this target only.

4. Confirm the new revision and start/restart the release workers. No additional package is needed; `cryptography` is already pinned in requirements. ProTrade's preceding download release requires no migration.
5. Live check: Admin Hub tile, an authorised invitation email, registration while signed out or signed in as the invited account, Submitted status, private review, approval/unique number, and a repeated-link rejection. Confirm Happiness's linked user ID and her existing shops/documents/token balance remain intact. Do not create a second account. Verify a ProTrade download and PM's existing folder workflow independently.

If rollback is needed, roll back application code while retaining the new records, sequence and encryption key. Destructive downgrade is intentionally blocked.

## Verification

- 9 registration/migration tests, including an actual local HTTP/Chromium form → CSRF-validated submission → redirect → Admin approval flow. Email was captured rather than sent. The real migration and constraints ran against session-temporary PostgreSQL tables in verified localhost `ait_local_db`; temporary relation checks include the sequence.
- Together with existing request-log and HOME/SACE credential-redaction checks: **19 tests and 23 subtests passed**, no failures/skips. The existing HOME logging fixture needed its current production `redacted_fields` dependency supplied; no HOME programme behavior changed.
- Existing ProTrade output/history, BOQ section download, pricing, preparation timer, quotation/invoice register and PM folder regressions: **32 tests and 5 subtests passed**, no failures/skips.
- The approval attribute was verified through an actual Alembic `EnvironmentContext`/`Config`, as used by the documented manual command. Unapproved migration execution was tested to fail before DDL. UI review was rendered and visually inspected using synthetic data.

Production SMTP delivery, Render secrets/migration, real printer completion and live user acceptance are outstanding deployment checks; they are not claimed as completed.

## Changed files

Routes/model/migration: `app/franchisees.py`, `app/models/franchisee.py`, `migrations/versions/ait_franchisee_001.py`.

Wiring/config/privacy: `app/__init__.py`, `app/models/__init__.py`, `app/logging_security.py`, `config.py`, `templates/admin/index.html`.

UI: `templates/franchisees/{_style,index,register,review,submitted}.html`.

Tests: `tests/test_franchisee_registration.py`, `tests/test_home_logging_security.py`.
