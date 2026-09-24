# Narrow RP journey and Board ? local, uncommitted

This stage preserves the earlier UIP safety and test cleanup. No commit, push,
deployment, production data change, schema change, or migration was performed.
MO/Treasurer workflows, shared startup/VisitLog, and historical failures remain parked.

## Existing components and blockers

| Step | Existing component reused | Finding / confined correction |
| --- | --- | --- |
| Welcome | `templates/public/welcome.html` | UIP tile uses the configured subject `about_url`; no shared Welcome change in this stage. |
| About / precinct selection | `GET /uip/` ? `public_about.html`; `GET /uip/<org>/about` ? `about.html` | Existing public About retained. Organization About now exposes Register / Select my role and is reachable before commercial entitlement. |
| Register | `POST /uip/select` ? existing `auth_bp.register` | Preserves `/uip/<org>/router?force=1`, so an existing membership does not skip the requested role-selection screen. Shared registration infers UIP from this URL; `register_decision` retains the continuation and bypasses individual payment for UIP. |
| Role Selection | `GET /uip/<org>/router?force=1`, `router.html` | Existing Ratepayer tile enters `verify_ratepayer`. No new role system. |
| Fork | `GET/POST /uip/<org>/verify/ratepayer` | Removed owner/resident-role shortcut, GET membership/role writes, verified/open claim writes, and manual-intake fallback. |
| Waiting Room | Existing `verify_ratepayer` route, new small `ratepayer_waiting.html` | Only unavailable authoritative Vault renders this page. No authority/claim/Vault records are written. |
| RP Board / property | Existing `/uip/<org>/ratepayer-workspace`, existing Board template | Replaced placeholder links and invalid governance-model import / nonexistent relationship `user_id` lookup with authoritative member ? ownership ? property lookup. |
| Mandates | Existing `/uip/<org>/public-mandates` | Board links to existing adopted-mandate register. No mandate redesign. |
| Lodge Query / My Queries | Existing Board route now accepts POST; existing `CoreInteraction` | No functioning RP municipal-fault submission route previously existed; Board query links were `#`. Added bounded submission to the same Board using `municipal_fault`, current organization, creator and recorded-by IDs. Own queries/status appear on the Board. |
| Photo evidence | Existing `UipDocument.interaction_id` and `UipDocumentVersion` | General document upload writes to `instance/uip_documents/<org>` and is staff-only. It is not reused for RP file persistence. RP evidence uses the existing R2 helper, while unrelated document storage is unchanged. |
| Vault | Existing `UipRegisterImport`, `UipMemberProfile`, `UipPropertyMember`, `UipProperty` | Lookup only; existing import service and records retained. No MO workflow built. |

## Defined fork and undefined business rule

- **No authoritative Vault available ? Waiting Room**. Availability uses existing municipal import batches with status COMPLETED/WITH_EXCEPTIONS and effective date no later than today.
- **Current authoritative match ? RP Board**, subject to the unchanged organization entitlement guard.
- **Vault exists but no current match ? fail closed (403)**. Business rule **not yet defined**. This does not route to the Waiting Room or manual intake and does not decide that the person is not an RP.

Matching uses one active municipal member profile with imported provenance and a
normalized account email match, excluding explicitly ineligible profiles. Property
access requires an active municipal property and an imported, verified owner
relationship valid today. Ambiguous, inactive, expired, future, manual, unverified
ownership and foreign-person records do not grant access. An application role alone
never proves Vault ownership. Navigation does not create/rebind membership or roles.
The pre-existing general dashboard/returning-role dispatch was not redesigned.

## Vault browser write boundary

The organization request guard denies existing member/property edit,
representative/ownership write and register-import endpoints, for all current UIP
users. Manual creation endpoints remain absent. The unsafe `vault-check` browser
listing is also denied. Register views expose no management controls and navigation
no longer offers Import Register. Existing offline import services remain unchanged;
this is not an implementation or authorization of a future MO ingestion workflow.

## Board and R2 evidence

The four functions are My Property (read-only), Mandates, Lodge a Query, and My Queries.
Queries use the current authenticated creator and organization; form values cannot
select another creator or organization. Query creation records `CoreAuditEvent`
with the actor and query ID, without copying query text into audit details.

Photo object path: **`uip/rp_queries/<uuid>.jpg|jpeg|png`** in the configured R2 bucket.
`UipDocumentVersion.storage_key` stores that object key; filename, content type,
size, SHA-256, actor, effective date and document/query association are PostgreSQL
metadata. No image bytes or hardcoded bucket/domain/credentials are stored there.

The existing `upload_file_to_r2` defaults remain compatible with organogram callers;
optional prefix/key-return arguments support RP evidence. A bounded JPEG/PNG upload
is decoded/validated in memory before upload. The default RP limit is 5 MB.
An authenticated, currently verified owner-scoped photo route reads the R2 object
into memory and returns it with private/no-store response headers. No permanent local
upload store was introduced. Existing Reading/LITRE helpers, media configuration and
persistent-disk videos were not modified. R2 bucket policy was not changed; the
application access checks do not claim to change bucket-level public access.

Invalid or failed photo uploads do not leave a submitted query or document record.
As with ordinary object storage, R2 and PostgreSQL do not share a transaction: a
later database commit failure after successful upload could leave an unreferenced
object. No general storage reconciliation/cleanup system was introduced in this stage.

## Validation and limits

Disposable local PostgreSQL only: `uip_test_safety`, port 55439, per-run isolated
schemas/rollback fixture. The normal local database and production were not used.

Final combined run: **98 passed**, no failures/errors:

- **28 focused RP tests**: both defined fork outcomes; safe undefined unmatched case;
  no navigation/Vault/authority writes; property display; own query/status;
  R2 upload/read and metadata; corrupt/oversized/unavailable photo behavior;
  cross-user photograph denial; browser write-route denials for all fixture roles;
  organization entitlement preserved; default R2 organogram helper compatibility;
  registration handoff and actual shared `register_decision` continuation.
- **1 existing public-mandate test**.
- **69 existing safety tests**, unchanged in this stage.

Command:

```text
python -B -m pytest --confcutdir=tests/uip tests/uip/test_ratepayer_journey.py tests/uip/test_public_mandates.py tests/uip/test_safety.py tests/uip/test_provisioning_final.py tests/uip/test_log_privacy.py tests/uip/test_bridge.py tests/uip/test_phase3_migration.py tests/uip/test_phase49_migration.py -q -p no:cacheprovider --tb=short
```

Output: `artifacts/uip-rp-final.txt`. Existing datetime/SQLAlchemy deprecation warnings remain.
The local environment was missing boto3; installed the existing requirements pin
`boto3==1.34.0`. Requirements were not changed.

**Test boundaries:** R2 transport is mocked; real credentials/bucket connectivity were
not exercised. Shared account creation/password/pledge forms and the shared application
factory are not run by the isolated UIP fixture. The tests execute real UIP pages,
PostgreSQL persistence, registration handoff, and the existing shared completion
function with enrollment creation stubbed; they are not a full-browser test of new
account creation. Welcome's configured UIP `about_url` remains a deployment dependency.
The full historical UIP suite was deliberately not run or repaired.

## Exact source/test/report files changed in this RP stage

1. `app/program_uip/__init__.py` ? browser Vault boundary; public About exemption.
2. `app/program_uip/routes.py` ? read-only fork, role-selection continuation, Board/query/photo routes, read-only register context.
3. `app/program_uip/completion_routes.py` ? hide browser import navigation.
4. `app/program_uip/services/ratepayer.py` ? new confined service reusing existing models.
5. `app/utils/cloudflare_r2.py` ? optional object prefix/key return and authenticated-caller object read helper.
6. `templates/program_uip/about.html` ? registration/role-selection CTA.
7. `templates/program_uip/dashboards/ratepayer_workspace.html` ? four functional RP sections.
8. `templates/program_uip/ratepayer_waiting.html` ? no-Vault Waiting Room only.
9. `tests/uip/test_ratepayer_journey.py` ? focused tests.
10. `docs/uip_rp_journey.md` ? this handoff.

Earlier unrelated and UIP uncommitted changes remain intact; the working tree contains
more files than this stage's list. No schema migration is required. Stop here.
