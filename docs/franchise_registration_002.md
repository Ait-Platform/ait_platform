# Franchise registration persistence and forward rename

## Production evidence (28 November 2026)

The user confirmed one production row, FR-0001 / user 644 (Bill), and no row for
user 645 (PRO). The existing `franchise_license` table is empty.
Read-only inspection of the existing Render service logs found the registration
POST at 19:58:53 SAST on 27 November returned HTTP 400, followed by navigation
away. The deployed handler returns 400 for field/terms/account-email validation
or CSRF rejection. Encryption configuration and SQL exceptions take 503/409
paths, not that observed response. No successful registration/commit was recorded
for that request. The existing logs do not retain which field/CSRF check failed;
do not invent the original value or attribute it to encryption or permissions.
The corrected response prominently says registration was not saved and lists
specific validation messages while retaining the submitted form values.

## Protection and compatibility

Previously, any nonempty FRANCHISEE_PII_KEY took precedence for writes, including
an invalid obsolete value. That is a separately reproducible configuration defect,
not established as the cause of the observed HTTP 400. New writes always use AIT's
existing secure shared SECRET_KEY, domain-separated by HMAC, and an explicit
`ait-shared-v1:` ciphertext marker. Legacy ciphertext is decrypted using its
original optional key, or the prior shared-secret fallback. New registration works
with the dedicated key absent or malformed. Retain the original key for Bill's
existing ciphertext; no code can decrypt a lost-key record without its key.
Duplicate identity detection checks available old/new keyed digests. Existing
unique database constraints remain unchanged. No PII, keys or complete URLs are
printed by the migration guard.

The canonical model uses `franchise_registration`. A narrow request-scoped legacy
mapper supports the old physical table before the approved migration. Its separate
metadata cannot create an obsolete table through application create_all. Ambiguous
or absent schema fails closed. New registrations can therefore be corrected and
deployed before the production rename is authorised, without startup DDL.

## Forward migration -- approval required; not executed

Target: `franchise_registration_002`; parent: `ait_franchisee_001`.
The sole DDL is ALTER TABLE ... RENAME TO. The original table OID is retained;
records, encrypted bytes, all IDs/dates/history, constraints, index objects,
foreign keys, owner/grants and sequence names/defaults remain unchanged. Existing
constraint/index/sequence names are deliberately retained. No licence table or
unrelated business/financial table is changed. The revision requires an explicit
Alembic approval attribute and cannot run from unapproved startup migration calls.

After deploying this commit, the prepared runner is READ-ONLY by default:

```sh
python scripts/franchise_registration_002_guard.py --expected-commit <full-approved-release-commit>
```

It uses only Render's assigned DATABASE_URL and verifies the recorded hostname,
actual driver hostname, database, session user, effective and active roles, schema,
port, PostgreSQL major compatibility, grants/ownership and all-head ancestry. DNS
and backend addresses are informational. It permits exactly the rename revision,
preserves all unrelated heads, and verifies Bill FR-0001 is present. It snapshots
all row values by digest plus original table OID, column metadata/defaults,
constraints (including incoming FKs), indexes, triggers, owner/ACL and number
sequence state. No app factory is loaded.

ONLY AFTER explicit production authorisation and confirmation of a current
recoverable backup, the same runner accepts these additional arguments:

```sh
python scripts/franchise_registration_002_guard.py \
  --expected-commit <full-approved-release-commit> \
  --apply \
  --authorisation 'AUTHORISE ONLY franchise_registration_002' \
  --backup-reference '<verified-export-reference-and-checksum>' \
  --backup-completed-utc '<ISO-time-with-timezone>' \
  --backup-recoverable-confirmed
```

The backup must be within 24 hours. One external transaction includes locks,
Alembic DDL/version changes and all post-checks. Lock timeout is 5 seconds and
statement timeout 30 seconds. Any failure rolls back the rename and version
update. No stamp, manual table creation, role changes or blanket upgrade heads.
No production execution is authorised by this document.

## Licence dependencies retained

`app/auth/routes.py` uses FranchiseLicense for the educational facilitator
Dashboard and for subject-specific student enrolment. It reads subject_id,
total_seats and used_seats, creates/links UserEnrollment records and increments
used seats. `templates/auth/franchise_dashboard.html` consumes those fields.
The model exposes user/subject relationships. An empty table is not proof that
these routes are obsolete. These dependencies are preserved; removal is deferred
pending the user's separate decision about educational licensing. The Global
Franchise programme list no longer queries licences or displays seat allocations.

## Verification limits

Tests use temporary PostgreSQL tables and synthetic users with IDs 644/645, not
production PII. They verify Bill's original FR-0001 and ciphertext survive the
rename, PRO can register with malformed/absent legacy settings, fresh visits show
the saved account, and editing/duplicates/isolation remain protected. Original
ProTrade clients and historical earnings remain unchanged. Real user 645 must
complete a valid production submission after deployment; the specific historical
validation field is not recoverable from the observed access log alone.
Folder Management remains on HOLD.
