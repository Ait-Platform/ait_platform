# Antigravity Rules for AIT Platform

## 1. Flask Templates (Critical)
- The base template for this project is named layout.html. 
- NEVER use base.html, with ONE EXCEPTION: The UIP module (app/program_uip) uses its own separate layout architecture and internal UIP pages extend program_uip/base.html.
- For all non-UIP internal pages, ALWAYS use layout.html when creating new templates or modifying existing ones.

## 2. Databases (Postgres Strict)
- There is NO SQLite fallback in this project.
- The platform strictly requires PostgreSQL.
- **Two Database Environment**: The project uses two distinct databases:
  - Remote (Render): it_platform_db (Live tests and real users).
  - Local (Desktop): it_local_db (Where AI agent test scripts execute).
  - **Production changes require explicit authorisation.** Follow [AIT Production Database — Verified Identity and Migration Protocol](#ait-production-database--verified-identity-and-migration-protocol) before production migrations or other database writes. This instruction file does not itself grant production-write approval.
- The connection is managed via the DATABASE_URL environment variable.

## 3. Persistent Disk vs Static Assets
- Static Assets: Any files required for the app to run (like SACE workshop slides, logos, CSS, JavaScript) must be placed in app/static/ and pushed via Git.
- Dynamic Uploads: User uploads must never be checked into version control.
- Current R2 rollout instruction (2026-09-23): deploy R2-first video delivery while preserving Render Persistent Disk as the fallback/master. Do not delete, modify or replace disk videos. Defer individual playback repairs until they are identified in the live application.
- Reading videos: destination bucket is `ait-platform-assets`, prefix `reading_videos/`, public domain https://pub-d9878fa5cbe44074bd45bb83a4376153.r2.dev. Both ordinary Reading and SACE Auditor delivery use R2 first, with read-only fallback to `static/uploads/reading_videos` on remote availability failure. Environment variables `R2_PUBLIC_DOMAIN` and `R2_READING_PREFIX` can override the defaults.
- Prepared local video repairs are not approved for upload as part of this initial deployment. The persistent disk must remain attached. See `docs/r2_disk_migration.md` for findings and outstanding verification.


## 4. Local Project Path
- The canonical local development directory for this project on the primary Windows machine is: D:\Users\yeshk\Documents\ait_platform

## 5. System Architecture (SACE Module)
- **R Page (Reneilwe / SACE Admin Page):** We will design an "R page" for the SACE Admin later. This page will host the Audit Logs (/sace/audit_report). Do not place the Audit Logs button on the participant or evaluator hubs.
- **Provider vs SACE Relationship (Critical Domain Context):** AIT is the *Provider*. We are giving SACE an activity for approval. The activities belong to the Provider (AIT), NOT SACE. SACE is merely an endorsement entity. When naming pages or UI elements, do not frame them as if SACE owns the activity (e.g., use "Provider's SACE Activities" rather than "SACE\'s Evaluation Hub").
- **Facilitator-Participant Sync:** The SACE workshop operates on a strict singleton room design (demo-session-1).
- **State Management:** The Facilitator Dashboard dictates the state (slides, lobby, active). The Participant App strictly polls /sace/workshop/get_state every 1 second.
- **Interaction Logging:** All participant votes (polls, reflections) are securely UPSERTED/INSERTED into the Postgres SaceWorkshopInteraction table. This creates a permanent, crash-proof audit trail for the SACE evaluators.
- **Terminology (Critical):** The physical machine/hardware is the "LITRE Blending Machine". However, the official SACE Provider Activity name is **"I Learn to Read English Using the LITRE Method"**. NEVER refer to the activity as "LITRE Blending Machine" or "Litre Reading" in the UI. Always use the full correct activity name.
- **Auditors vs Evaluators:** When dealing with SACE's endorsement process, always refer to the individuals as "Auditors" (e.g. use "Provisioned Auditors"). You can provide examples of Auditors acting as "evaluators", but the official noun must be Auditors.
- **Auditor Access Codes:** SACE evaluators do not receive emails to log in. The admin generates single-use access codes on the Control Centre and hands them out. Auditors join via /sace/join using these codes.
- **Routing Rules:** 
  - The Participant Check-In is completely separated into /sace/participant/join.
  - The Interactive App (/sace/workshop/interactive) contains NO lobby HTML. If a user hits it without joining, they are bounced back to the join route.
- **Render Deployment Constraint:** Render takes 5-9 minutes to deploy. **Code Freezes** are required before live SACE evaluation demos.
  - **Communication Note:** The user uses the acronym **"wew"** (While We Wait). This means we are waiting 4-9 minutes for Render to deploy. During a 'wew' phase, we look forward and plan what to do next without writing any code. 



## 6. Payment Portal
- Paystack is AIT's sole target payment portal.
- Do not introduce or extend Yoco, PayFast, Stripe or any other payment provider.
- Existing non-Paystack integrations are legacy technical debt. Remove them only through separately tested cleanup after confirming no active dependency.
- When creating subjects or modifying payment constraints, ensure processor_default uses paystack.


## 7. Template & UI Layout Rules
All templates must strictly follow this Tailwind format:
1. **Base Layout**: Must {% extends 'layout.html' %}.
2. **Tile Container**: All content must be inside a central tile/card (e.g., a white div with shadow and rounded corners).
3. **Color Strip**: The top of the tile must have a color strip matching the subject/welcome page color.
4. **Row 1 (Header)**: The title must be on the left, and a Back button on the right.
5. **Row 2 (Actions)**: Any other primary action buttons should be in row 2, right-aligned.
6. **Flash Messages**: Flash messages MUST be rendered *inside* the tile content area, not outside it.
7. **Forms & Textboxes**: All textboxes must have clear outlines (order border-slate-300) so the user sees where to type.
8. **Autofocus**: The cursor must automatically focus on the first textbox (utofocus attribute).

## 8. Architectural Decision Making
- Always default to the industry best-practice architectural approach. Do not present multiple options to the user if one is clearly the standard best practice; just implement the best practice and explain why.


## 9. Certificates & Post-Test
- **NEVER** build custom HTML certificates for download.
- **ALWAYS** use the standardized _generate_certificate_pdf and _email_certificate_pdf functions (e.g., from pp.subject_reading.routes) to generate the official AIT PDF and email it to the user.
- **Email Delivery:** Instead of direct download buttons, provide a form where the user can confirm/enter their email address to have the certificate sent to them.

## 10. Strict UI Adherence
- **CRITICAL:** Re-read and strictly follow Rule 7 for all new templates. Do not deviate with custom Tailwind structures (e.g., placing titles outside the main tile, or skipping the back button row). 


## SACE Audio Generation Guidelines
When generating TTS (Text-to-Speech) audio for SACE Endorsement slides or modules:
- STRICT DICTATION ONLY.
- Do NOT add conversational narrative, filler words, or extra commentary (e.g., "Welcome to...", "As you can see...").
- The audio must be a 1:1 reading of the slide content.


## Program-first delivery / park unrelated infrastructure

AIT contains legacy and cross-program technical debt. Keep program work focused on getting that program operational and deployable end-to-end. Document unrelated migration, startup, schema and infrastructure issues as **PARKED** debt in this file or the appropriate project documentation. Do not expand into platform-wide cleanup unless an issue directly prevents safe testing or deployment. Prefer the smallest safe isolation/workaround; never silently modify another program to make the current one work. Preserve discovered blockers for later work. Success means working programs, not eliminating all historical debt first.

**PARKED STARTUP SAFETY:** `create_app()` contains automatic database mutation/maintenance blocks outside the existing `SKIP_AUTO_MIGRATE` guard, including unrelated Mech Shops, CRM and SPV maintenance. This needs a dedicated startup-safety cleanup later. Do not broaden Retirement work into it unless unavoidable for safe deployment. Use an isolated Retirement test context to avoid those startup mutations.


### Database Credential Source of Truth

- Use the Render service's assigned `DATABASE_URL` for production work. The current
  Render PostgreSQL resource configuration is the authority for credentials.
- Never obtain production credentials from maintenance or diagnostic scripts,
  scratch files, Git history or hard-coded connection strings.
- Local `.env` is intentionally local and points to `ait_local_db`. Do not replace
  it with the production URL or permanently store production credentials there.
- If external production access is separately required and authorised, obtain
  its connection configuration from the current Render PostgreSQL resource.
- If authoritative connection configuration is unavailable, stop and access the
  current Render resource configuration; do not fall back to historical scripts
  or repeatedly request the identity parameters already recorded here.
- Historical retirement preflight notes recorded a failed login using an obsolete
  connection configuration. Those historical login assumptions must not override
  the verified session/login and effective-role identities recorded below.
- Legacy credential references in maintenance, diagnostic and scratch scripts
  remain PARKED debt. They are not credential sources; their cleanup is separate.

## AIT Production Database — Verified Identity and Migration Protocol

Verified on **27 November 2026**. This protocol applies across **all AIT
programmes**. These are verified identity parameters, not credentials or standing
permission to modify production.

| Parameter | Verified value |
| --- | --- |
| Hosting platform | Render |
| Database hostname | `dpg-d4bkqsf5r7bs73989ia0-a` |
| Database name | `ait_platform_db` |
| Session/login user | `aitplatformdb_retirement` |
| Effective database role | `ait_platform_db_user` |
| Active role | `ait_platform_db_user` |
| Schema | `public` |
| Port | `5432` |
| PostgreSQL version at verification | 18.3 |

### Mandatory migration protocol

1. Use the Render service's assigned `DATABASE_URL`.
2. Do not request database identity details repeatedly when the verified
   information is already recorded here. Use read-only runtime checks to verify
   the recorded identity. A mismatch must stop the operation for review; do not
   silently substitute another identity or weaken a guard.
3. Independently verify `session_user` and `current_user`. Their difference is
   expected: the session/login user and effective role have distinct identities.
   Also verify the database, active role, schema and port against the recorded
   values. Do not issue `SET ROLE` merely to make an identity guard pass.
4. Verify both the configured hostname and the actual driver connection hostname.
5. Treat differences between DNS addresses and PostgreSQL backend IP addresses
   as informational diagnostics. Render's private network addresses may differ;
   IP equality must not be a blocking identity assertion. When parsing PostgreSQL
   inet text containing a prefix, use `ipaddress.ip_interface(value).ip`.
6. Do not hardcode transient IP addresses as permanent identity guards.
7. Verify migration ancestry across all existing Alembic heads, including
   `down_revision`, `depends_on` and merge ancestry. A programme's applied
   revisions can be ancestors of a head named for another programme. Do not
   infer unapplied migrations merely from the absence of a programme-named head.
8. Verify the effective role's necessary migration permissions and physical
   schema compatibility. Check ownership rights where DDL requires them, plus
   relevant table, reference and sequence privileges. Permission or schema
   failures stop execution; changes to roles, grants or Render configuration
   require separate authorisation.
9. Confirm a recoverable production backup before executing migrations.
10. Obtain explicit authorisation for the production migration plan, including
    every outstanding prerequisite, before executing production migrations.
11. Preserve unrelated migration heads. Use the explicitly authorised target;
    do not use a blanket `upgrade heads` or stamp past an unexplained mismatch.
12. Execute migrations transactionally where supported and verify the results,
    including the resulting heads, required schema and preservation of existing
    data. Avoid application startup maintenance when running migration tooling.

The PostgreSQL version above records the verification date's version; evaluate
compatibility if it changes rather than treating that snapshot as a permanent
version pin.

Never store database passwords, complete connection strings or other credentials
in repository documentation. Do not print credentials into reports or logs.

This protocol does not authorise production writes or lift programme scope holds.
**Folder Management remains on HOLD.**


## Mandatory Global Output and Global Email architecture

### Global Output

Global Output must provide a common, reusable interface for generating, viewing
and distributing current documents across all AIT programs.

The mandatory interface is:

- Header: Program Output with a Back button.
- Row 1: Program-specific entity selection, Category and Report/Document.
- Row 2: By Month, Date From, Date To and Preview, with Preview right-aligned.
- Row 3: Print, Download PDF and Email, right-aligned.
- Document preview immediately below these controls.

Selection fields must adapt to the calling program. Disable irrelevant date
controls. Preview, Print, Download PDF and Email must use the same document
content. Download PDF must use normal browser downloads without directory
authorisation. Email must invoke the Global Email module. Historical financial
documents must retain their original values. Right-align numeric columns and
preserve program-specific permissions and data isolation.

Use one reusable Output framework across AIT. Individual programs supply their
own document types, selection parameters, data and report-generation logic.
Avoid duplicating Output interfaces and common processing logic. Global Output
must remain independent of Global Folder Management and Franchise. Future folder
integration must not interfere with existing Output functionality.

### Global Email

The successfully live-tested ProTrade Email behaviour is the mandatory functional
standard for all AIT programs. The Global Email module must provide:

- An editable default recipient, subject and message.
- The selected PDF attachment, Send and Cancel buttons.
- Duplicate-send protection and clear success and failure messages.
- Support for documents that do not require a debtor.
- Reusable invocation from Global Output and, eventually, Global Folder Management.

A recipient override applies only to that email; it must not change the registered
contact. Preserve authentication, ownership, CSRF validation, attachment validation
and truthful uncertain-delivery messages. Individual programs must not develop
separate email senders where the Global Email module can be used.

### Current implementation and deferred centralisation (27 November 2026)

The standard above is mandatory architecture, not a claim that full centralisation
already exists. ProTrade currently owns `templates/program_mechanic/output.html`,
`app/static/js/protrade_output.js` and `app/protrade_output.py`. PM owns its separate
`templates/program_billing/utilities_output.html`, `pm_utilities_output.js` and
utilities Output/report adapters. Their selection, preview, printing, download,
email invocation and receipt orchestration remain program-specific.

Already shared are `templates/partials/email_composer_fields.html` (used by
ProTrade and other email pages), `app/utils/mailer.py` including
`send_standard_report_email`, and the HTML-to-PDF renderer in
`app/utils/pdf_render.py`. PM Utilities Output currently calls `send_pdf_email`;
ProTrade calls `send_standard_report_email` from that shared mailer module.
These shared pieces are not yet one complete Global
Output/Email controller. Future authorised centralisation should extract the
common Output interface and action handling and reusable Email invocation,
validation, duplicate-send/status lifecycle, while retaining program-specific
report adapters, permissions, snapshots and durable receipt associations.
Preserve the working ProTrade and PM workflows; this documentation does not
authorise broad refactoring, PM changes or Folder Management development.

AIT token charges must ultimately be configurable through Admin Hub. Consultants
remain responsible for setting their own customer charges. Do not publish fixed
AIT charges, suggested customer prices or operator remuneration arrangements in
ProTrade Help. This rule does not authorise changing the token charging engine,
wallet functionality or Paystack integration.

## Mandatory Global Franchise Management standard

Franchise Management is a global AIT module alongside Global Output, Global Email
and Global Folder Management. Franchise development and rollout are authorised
from 27 November 2026. Folder Management remains on HOLD; this authorisation does
not change any programme's financial, professional or regulatory permissions.

- Participation is open and voluntary for every registered AIT user. The Bridge
  Franchise tile opens registration for a new participant and the existing
  Franchise Dashboard for an existing participant.
- Register once using the existing AIT account, a simple profile and explicit
  acceptance of the applicable AIT terms and Franchise participation conditions.
  Record acceptance version, actor and timestamp in the existing audit history.
  Allocate the existing unique registration number and activate immediately.
- No invitation, applicant screening, Admin approval or manual activation is a
  prerequisite. Legacy invitation and review records remain preserved; historical
  decisions, issued numbers and audit history must never be rewritten on entry.
- One franchisee identity may participate across ProTrade, educational programmes,
  Property Management and future programmes. Reuse existing programme entry and
  licensing mechanisms; registration does not automatically grant programme
  enrolment, professional accreditation, regulatory approval or SACE delivery
  authority. SACE application outcomes do not govern Franchise registration.
- Administration is database-driven: retain one registration per account, unique
  numbers, client ownership and isolation. Admin Hub provides visibility of profile,
  status and history, with audited suspension and reinstatement as exceptions.
  Suspended participants must not reactivate themselves by submitting Register.
- Use trust-based registration with administrative accountability. Do not create
  Franchise-specific environment activation switches. New self-registration works
  with AIT's existing shared secure application configuration; no Franchise-specific
  Render setting is required. Preserve optional legacy PII decryption keys for
  existing ciphertext and legacy invitation configuration where still retained;
  these are compatibility/security data, not activation switches. Never remove
  keys required to read existing encrypted records.
- Reuse `franchise_registration`, its preserved number sequence and audited status
  model. Internal legacy `Approved` represents active registration, not a manual
  approval prerequisite. Suspension/reinstatement remain audited history events.
  Preserve existing educational `FranchiseLicense` records and programme access.
- Do not introduce fixed franchise fees, commissions or remuneration. Future
  commercial controls must be configurable through authorised administration.
  Existing historical earnings remain read-only at their original saved values.

All Global Franchise Management tables must use the `franchise_` prefix. The
forward-only `franchise_registration_002` migration renames
`ait_franchisee_registration` to `franchise_registration` without rebuilding it.
Preserve all existing rows, IDs, numbers, registration dates, ciphertext, audit
history, constraints, indexes, grants, relationships and sequence values. Published
migrations remain immutable. The application supports the physical legacy name
until the separately authorised rename is applied; it never creates or migrates
tables during registration.

New registration and profile writes use versioned encryption derived from AIT's
existing secure shared application configuration. No Franchise-specific Render
variable is required for new registration. Original untagged ciphertext remains
readable with its original legacy key; preserve that key for legacy reads and
never silently replace it, rotate existing ciphertext or claim encrypted data can
be recovered without the key. Malformed legacy configuration must not gate a new
shared-key registration. Retain duplicate email/account/number protection and
report field, terms, CSRF, duplicate or database-permission errors clearly. Do not
announce registration success before commit. Trust first, accountability
thereafter: one registration per AIT user, automatic activation and audited
administrative suspension/reinstatement; programme-specific regulatory
requirements remain separate.

### Simplified Franchise registration and editing (28 November 2026)

- Do not collect an SA ID number in registration or editing. There is no identity
  checksum validation, mandatory identity value, identity digest generation or
  duplicate identity check. Never fabricate placeholder identity data.
- Do not display an email textbox or accept an email value from a posted form.
  On every successful registration/profile Save, use the authenticated user's
  existing normalised AIT account email and stable user ID. Changes to that AIT
  account itself remain outside the Franchise form.
- Retain full name, cellphone, encrypted residential address, optional occupation
  and explicit terms acceptance for once-off registration. Activate automatically,
  allocate a unique franchisee number, enforce one registration per AIT user and
  show the committed record immediately in the Franchise Register.
- Preserve historical encrypted identity data and digests byte-for-byte. Keep the
  historical columns and authorised administrative viewing; neither registration
  nor editing may decrypt, clear, replace or validate those identity values.
  Preserve numbers, original registration dates, history, ownership and suspension.
- The approved forward migration `franchise_registration_003` removes the
  identity NOT NULL conditions from `ck_franchisee_submitted` and removes only the
  obsolete identity-digest unique constraint/index. User/email/number uniqueness,
  foreign keys and all other constraints remain. No existing record is rewritten.
- Revision 003 follows the separately authorised 002 rename. Do not modify the
  published migrations or execute either in production without explicit approval
  and the recorded identity/backup/transaction protocol. Until the required schema
  is present, show a clear pending-migration message and prevent a new registration
  from submitting values that the old identity constraint would reject.
- Preserve old invitation records/URLs, but direct profile entry through the
  authenticated account-based form. No anonymous form or posted email may choose
  the account receiving a registration.

No franchise licence, invitation, allocated seat or prior approval is required
for Global Franchise registration. `franchise_license` currently has a separate
educational dependency in `/franchise/dashboard` and
`/franchise/register_students`: it binds subjects and controls student enrolment
seat allocation. Preserve that table, fields, routes and existing records pending
a separately authorised educational licensing review. Global Franchise does not
query or require it for registration or its Available Programmes interface.

### Franchise dashboard business activity (28 November 2026)

- For registered franchisees the menu contains only **Available Franchise
  Programmes | Client Register | Output** as three equal-width buttons. Retain
  Register for unregistered participants
  and retain the saved Franchise Register with profile Edit. Remove Earnings and
  ProTrade operational dashboard interfaces, preserving all underlying earnings,
  financial records, registration and administrative functionality.
- Available Franchise Programmes is an explicit Franchise offering list, currently
  **ProTrade only**. Do not populate it from the general AIT subject catalogue,
  personal enrolments or educational seat licences. Further programmes require
  separate authorisation; this listing does not grant operational access.
- Client Register is a read-only business activity summary, never a second
  operational dashboard. Show one row per owned ProTrade shop with Client / Shop,
  Trade, Quotations, Invoices, SOAs and Tokens Used; right-align numeric columns
  and show a bold TOTAL row. Do not provide Open buttons or programme navigation.
- Count completed quote/invoice/SOA receipts in `protrade_document`; exclude drafts,
  previews and repeat access to the same receipt. Sum original recorded `tokens`
  across all shop receipts, including BOQ section charges, without applying current
  tariffs. Make the recorded-receipt basis visible; do not invent counts or charges
  for historical documents lacking receipts. Preserve all historical records.
- Independently scope receipts and shops to the authenticated franchisee's stable
  AIT user ID. Keep same-named shops separate and exclude orphaned, unassigned or
  mismatched-owner receipt associations. Empty shops show zero activity; missing
  clients and inactive/suspended registrations show meaningful messages. Viewing
  this summary must never generate documents, debit tokens or write financial data.
- Folder Management and country-based architecture remain on HOLD.

### Franchise Output (28 November 2026)

- Keep existing Dashboard font sizes; use dark slate text for headings, helper
  text, form values and tables. Retain semantic error/success indications.
- Franchise Output follows the AIT three-row standard: owned franchisee, Category,
  Report / Document; By Month, Date From, Date To and right-aligned Preview; then
  right-aligned Print, Download PDF and Email. Client Activity Report is cumulative,
  so date controls are disabled. Preview is immediately below these rows.
- Use the same `client_activity()` adapter for Client Register and the report,
  including original token values, exact totals and strict user/shop isolation.
  Include franchisee name, number and report date; right-align numeric columns
  and make the TOTAL row bold. Never deduct tokens or change activity records.
- Reuse `partials/output_controls.html`, `ait_output_actions.js`, the shared
  `report_output.py` preview/email helpers, existing HTML-to-PDF renderer, shared
  email composer fields and `send_standard_report_email`. This is incremental
  shared-component reuse, not a claim that PM and ProTrade controllers are fully
  centralised. Preserve their workflows; future programme adapters can consume
  these controls and helpers without duplicating transport code.
- Bind previews to authenticated user, registration and a signed data digest with
  a 30-minute expiry. Reject changed report/profile data before export/send and
  require a new Preview. Render the same report template/date for preview, print,
  PDF download and attachment. Downloads use the browser normally, with no folder
  permission requirement. No Folder Management integration is authorised.
- Reuse the registration's existing append-only audit history to reserve email
  attempts under a row lock before SMTP. Record nonce and outcome only; preserve
  all existing history. The durable reservation blocks repeated submissions of
  that preview across workers, including uncertain delivery or outcome-save
  failures. Editable recipient, subject and message apply only to the send;
  never change registered contact details. No new schema is required.
- Only the Output route permits same-origin action scripts; sensitive profile
  forms retain their script-disabled CSP. Require current active registration,
  ownership and CSRF for email. Transport acceptance is not inbox delivery.

## Deferred centralised Folder Management

AIT must have a centralised Folder Management module responsible for folder selection, authorisation, accessibility verification, replacement, automatic subfolder creation, document saving and historical document retrieval. Individual AIT programs must consume this shared module rather than maintain independent implementations.

ProTrade Folder Management is temporarily suspended: hide Folders in all ProTrade
menus and Save Report in Output. Preview, Print, Download PDF and the standardised
email sender operate independently of local folder registration or authorisation.
Preserve existing folder code, records, registered handles and historical documents.
Property Management and its existing Folder Management remain unchanged.
Folder Management development remains on HOLD; this requirement
does not authorise implementation of the centralised module.

**Historical exception (revoked 27 November 2026; retained for reference):** ProTrade may provide Main
Directory selection, read/write verification and explicit replacement, and create
`registered program / consultant / newly registered shop / Invoice, Statement`
through the existing shared directory store and owner markers after successful
shop registration. Do not backfill existing shops or create Quotes separately.
Output saving and broader Folder Management remain suspended. Do not modify PM
workflows, PDF operations, email or historical retrieval for this exception.

### Unresolved ProTrade Windows directory failure - HOLD

Live Windows testing failed again at commit `948f064a`. Earlier diagnostics
(`3baf2fe6`) confirmed successful server authorisation, IndexedDB handle retrieval,
shop ownership validation and lock acquisition, then stalled at
`Shop creation: query main-directory permission` for at least 165 seconds while
the creation lock remained held. The `948f064a` bounded-permission and
fresh-registration recovery changes passed automated tests but did not resolve
the reported live failure. An obsolete registration was investigated; its causal
role and the underlying Windows/browser failure remain unconfirmed.

All ProTrade Folder Management is now suspended, including Main Directory,
legacy folder routes, automatic folder creation during shop registration,
automatic permission checks and diagnostic processes. Shop registration must
return to its normal workflow without directory selection or authorisation.
Retain Preview, Print, Download PDF and Email independently. Preserve all folder
code, diagnostic findings, records, registered browser handles and historical
files. Do not clear registrations or delete, migrate or rewrite existing folders.
Property Management is unchanged. Folder Management remains on
HOLD until explicitly authorised to resume; the historical exception above no
longer authorises execution or development.

## Permanent filesystem and template naming conventions
- Legacy subjects use the `subject_` filesystem/template prefix.
- Programs use the `program_` filesystem/template prefix.
- All new standalone programs must follow `program_<slug>`.
- Public URLs, `auth_subject` slugs, database table names and business identities do not need the filesystem prefix.
- UIP and Retirement retain `/uip`, `/retire`, `uip_bp`, `retire_bp`, existing catalogue slugs, tables and business behavior.


## RCM / UIP — FROZEN AUTHORITY, ONBOARDING AND COMMERCIAL ARCHITECTURE

======================================================================
1. COMMON PLATFORM RULE
======================================================================

AIT authentication establishes user.id only.

Each program independently establishes:

    program-specific authority
        ↓
    organisation relationship
        ↓
    relationship / role
        ↓
    permissions/functions
        ↓
    working context
        ↓
    dashboard

Do not treat authentication itself as organisation membership or role authority.

UIP and RCM remain separate business domains and must not share business
organisation, membership, role, entitlement or operational records merely
because their UI concepts are similar.

======================================================================
2. ORGANISATION-FUNDED COMMERCIAL MODEL
======================================================================

RCM and UIP are organisation-funded programs.

RCM:
    The retirement home organisation pays for RCM.
    Example:
        Archoney House
        Samaritan Home

    Residents, nurses, staff, family/representatives and other individual
    users do NOT individually purchase RCM.

UIP:
    The UIP organisation pays for UIP.

    Ratepayers, elected members, staff, providers and public users do NOT
    individually purchase UIP.

Commercial entitlement must remain distinct from individual membership,
relationship, governance authority and role.

Paystack is AIT's approved payment processor where payment is required.

======================================================================
3. RCM ORGANISATION MODEL
======================================================================

RCM = Retirement Centre Management.

RCM is the AIT program.

Archoney House, Samaritan Home, etc. are client organisations operating
inside RCM.

An owner creates/registers their retirement home inside RCM; they do not
"create RCM".

One AIT user may belong to multiple retirement homes.

All relationships, roles, permissions and operational data are home-specific.

No data or authority may leak between homes.

======================================================================
4. RCM FIRST FORK
======================================================================

After About:

    HOW WILL YOU USE RETIREMENT CENTRE MANAGEMENT?

        OWNER / ORGANISATION ADMINISTRATOR
            → create a NEW retirement home

        OTHER
            → person belongs to or requires authorised access to an
              existing retirement home

This is a relationship/onboarding fork, not final operational role selection.

An Other user must never gain home-creation authority merely because no home
currently exists.

======================================================================
5. RCM WAITING ROOM = PERMISSION
======================================================================

RCM Waiting Room means:

    "I am waiting for permission to enter a retirement home's private
     digital environment."

Frozen waiting-user discovery information:

    - confirmed AIT name / recognisable preferred name
    - retirement-home name if known, optional free text
    - explicit consent to limited discovery by authorised RCM home admins

Do NOT require locality merely for discovery.
No access code is required for this initial mechanism.

A waiting user may exist before any retirement home has been created.

Example:

    Mary → Other → Waiting Room

Later:

    Graham → creates Archoney House
    Joanne → creates another home

Authorised home management may discover Mary.

Graham:
    "Not ours"
        → applies to Archoney House only.

Joanne:
    approves Mary
        → establishes permission/association with Joanne's home only.

One home's denial must never globally reject Mary from RCM or affect another
home's decision.

"NOT OURS" RECONSIDERATION - FROZEN:

A retirement home may explicitly reconsider its own previous "Not ours"
decision through an authorised action, provided the waiting user still
consents to discovery at the time of reconsideration.

The original decision remains permanently auditable. Reconsideration appends
history; it never overwrites history. Editing Waiting Room information does
not automatically reopen a home's decision.

Each home's decisions remain private and independent. "Not ours" is never a
global denial. Reconsideration grants no relationship, role or operational
permission beyond the home association being approved.

RCM WAITING ROOM / HOME ASSOCIATION — FROZEN
======================================================================

1. retirement_waiting_user

RCM Waiting Room identity exists independently of any retirement home.

One authenticated AIT user has at most one current RCM Waiting Room identity.

It contains only the limited discovery information already frozen:

- authenticated user identity;
- confirmed/recognisable preferred name;
- optional retirement-home name clue;
- discovery consent;
- appropriate consent/update timestamps.

It has no retirement_organisation dependency.

The optional home name is a clue only and establishes no authority or
association.

2. Discovery consent

Discovery consent controls whether authorised retirement-home management may
discover/review the Waiting Room user through the discovery process.

Withdrawal of discovery consent:

- removes the user from further discovery;
- prevents new discovery-based approval/reconsideration while consent is absent;
- does NOT delete the Waiting Room audit/history;
- does NOT revoke an already-approved home association;
- does NOT revoke an independently established relationship, role or authority.

Association revocation, relationship withdrawal and role withdrawal are
separate authority processes and must never be inferred from discovery-consent
withdrawal.

3. retirement_membership final meaning

retirement_membership is the durable:

    person
       ↕
    retirement home

association identity.

Preserve:

    UNIQUE(organisation_id, user_id)

Membership existence alone grants NO operational authority.

Do not create a second permanent person-home association entity.

4. Home-association approval

Home permission is represented independently of the legacy operational
membership lifecycle.

Add explicit positive association approval facts to retirement_membership:

    association_approved_at
    association_approved_by_user_id

These fields represent:

    "This retirement home has approved this person's association with the home."

They do NOT represent:

- Staff;
- Resident;
- Family / Representative;
- staff role;
- operational permission;
- dashboard entitlement;
- commercial entitlement.

For a NEW Waiting Room approval:

- create/reuse the unique person-home membership as appropriate;
- populate association approval facts;
- leave legacy status NULL for a new association-only row;
- leave requested_role_id NULL;
- leave approved_role_id NULL;
- append the immutable association-review event;
- commit association approval and audit event atomically.

NULL legacy status is NOT a new business status.

It means the legacy operational membership lifecycle is not being used for
that association-only row.

Do NOT call or display it as:

    associated
    incomplete
    access_setup_incomplete

or another invented lifecycle state.

5. Legacy coexistence

Existing legacy membership IDs, statuses, role assignments, review fields and
history must be preserved.

Stage 2 discovery approval must not silently alter:

    pending
    active
    denied
    disabled

legacy operational decisions.

Existing operational gates remain in place during Stage 2.

Association-only membership therefore does NOT gain operational access.

Where trustworthy historical evidence supports association approval, controlled
backfill may record it.

Never fabricate historical reviewer identity or approval dates.

Where evidence is insufficient, preserve the legacy row and require later
explicit reconciliation.

6. retirement_association_review

retirement_association_review is immutable home-specific audit/history.

It records:

- home;
- Waiting Room/person identity;
- decision;
- authenticated reviewer;
- timestamp;
- decision/reconsideration history and required concurrency/version data.

It is NOT the source of operational authority.

An approval event and the durable membership association approval must commit
atomically.

"Not ours":

- creates no membership merely because of rejection;
- applies only to that home;
- remains auditable;
- hides/resolves the candidate for that home's normal unresolved discovery;
- does not affect another home's discovery or decision.

Explicit reconsideration is permitted while discovery consent exists.

Reconsideration appends history and never overwrites the original decision.

7. Stage boundary

Stage 2 establishes only:

    Waiting Room identity
        ↓
    limited discovery
        ↓
    home-specific decision
        ↓
    durable approved person-home association

Stage 2 does NOT establish:

    relationship
    staff role
    operational permission
    Acting-as context
    commercial entitlement

Those remain later stages.

======================================================================

======================================================================
6. RCM INITIAL OWNER AUTHORITY
======================================================================

An authenticated user may create a NEW RCM organisation through the Owner path.

The creator makes an explicit declaration that they are authorised to establish
and administer that retirement home's RCM organisation.

That establishes the creator as founding Organisation Owner/Admin.

This is authority within RCM.

It is NOT AIT certification of legal ownership of the physical retirement
home.

Entering the same or similar name must never permit takeover of an existing
RCM organisation.

MULTIPLE FOUNDED HOMES - FROZEN:

The same authenticated AIT user may found more than one RCM retirement-home
organisation. Each NEW home requires its own explicit founding-authority
declaration.

Example:
    Graham -> Archoney House -> founding Owner/Admin
    Graham -> Second Home -> founding Owner/Admin

This does not permit name-based takeover, claiming, merging or access to an
existing RCM organisation.

UNIQUE(retirement_organisation.owner_user_id) and route assumptions that an
owner can create only one home are NOT part of the target architecture.

Existing organisation IDs, membership IDs and review history must be preserved
when implementing this decision. This records design only; it does not itself
authorise application or schema changes.

======================================================================
7. RCM RELATIONSHIP THEN ROLE
======================================================================

After a home approves a person:

    Home permission
        ↓
    Home relationship
        ↓
    Operational role where required
        ↓
    Permissions/functions
        ↓
    Dashboard

Frozen high-level relationships:

    STAFF
        → requires explicit operational role(s)

        Examples:
            Facility Manager
            Nurse / Care Staff
            Administration / Reception
            Finance
            Kitchen / Catering

        One Staff relationship may hold multiple explicitly assigned roles.

        Do not manufacture combined roles or choose a "highest" role.

    RESIDENT
        → legitimate relationship in its own right
        → does not require an invented staff role

    FAMILY / REPRESENTATIVE
        → legitimate relationship in its own right
        → appropriate resident association and authorised scope must later
          be established
        → family connection alone does not grant resident-data access

A person may hold multiple independently authorised relationships within the
same home.

Example:

    Mary → Archoney House
              ├── Staff → Nurse / Care Staff
              └── Family / Representative

Changing/removing one relationship must not automatically alter another.

"Approved, access setup incomplete" is currently a BUSINESS CONDITION only.
Do not prematurely encode it as a prescribed database status.

STAGE 3 AUTHORITY - FROZEN:
Withdrawing Staff atomically closes every active role assignment beneath that
Staff grant. Preserve all history and leave Resident and Family / Representative
unchanged. Regranting Staff never revives old roles; explicitly grant roles again.
Current authority comes from unwithdrawn relationships and, for Staff, unwithdrawn
role assignments. Immutable authority events provide audit/concurrency evidence,
never current authority through event replay.
Owner/Admin authority is separate from Staff and implies no operational role.
Legacy active Staff authority requires evidenced migration before cutover.
Unresolved authority blocks cutover. Preserve all legacy fields/history; never
fabricate historical authority. Pending/denied/disabled rows are not automatically
authorised. After cutover, legacy approved_role_id cannot grant Staff authority.

======================================================================
8. RCM MULTIPLE HOMES AND WORKING CONTEXT
======================================================================

Relationships and roles belong to the home relationship, not globally to
user.id.

Example:

    Mary
      ├── Archoney House
      │      Staff → Nurse / Care Staff
      │      Family / Representative
      │
      └── Samaritan Home
             Staff
               ├── Nurse / Care Staff
               └── Facility Manager

Valid RCM working context:

    Home
      +
    Relationship
      +
    authority required by that relationship

Entry rule:

    one valid home
        → enter it automatically

    multiple valid homes
        → ask which home

Within selected home:

    one valid relationship
        → enter it automatically

    multiple valid relationships
        → ask "Acting as"

Multiple Staff roles inside one Staff relationship do NOT create another
identity-selection step.

The Staff dashboard may expose functions from all explicitly assigned Staff
roles without inventing a combined role or "highest" role.

Pending/incomplete relationships are not usable working contexts.

======================================================================
9. RCM CONTEXT ISOLATION
======================================================================

Every operational request must remain explicitly bound to:

    retirement home
        +
    relationship

Changing home resolves authority afresh for the selected home.

Changing "Acting as" retains the home but changes relationship context.

Never carry home-specific records, selected residents, unfinished actions or
other operational state into another context.

A bookmarked URL or another browser tab must remain bound to its own explicit
context.

Do not rely on a single mutable global "current home" value that could silently
reinterpret an old URL or another browser tab.

The context selector grants no authority.

Protected functions must independently enforce the authority represented by
the requested context.

Fresh RCM entry:

    if multiple valid contexts exist
        → ask again

    if only one valid context exists
        → enter directly

Within ordinary navigation, retain the explicitly selected context.

======================================================================
10. UIP FUNDAMENTAL AUTHORITY DISTINCTION
======================================================================

Do NOT copy RCM permission-based entry into UIP.

A UIP exists externally through its applicable governance/municipal process.

The UIP Admin establishes/configures that already-existing UIP on AIT.

They do not create the real-world UIP.

======================================================================
11. UIP RATEPAYER = VAULT ENTITLEMENT
======================================================================

The authoritative RP list in the Vault establishes the ratepayer relationship
and applicable UIP.

The Secretary does NOT approve or reject ratepayer entitlement.

    authenticated user
        ↓
    authoritative RP/Vault match
        ↓
    correct UIP
        ↓
    automatic entry

If the required authoritative data cannot yet establish the relationship:

        → UIP Waiting Room

UIP Waiting Room therefore means:

    "AIT cannot yet establish the authoritative relationship."

It does NOT mean:

    "Waiting for Secretary permission."

Once the Vault establishes the ratepayer relationship, entry is automatic.

A legitimate Vault-established ratepayer cannot be excluded merely because
UIP administration considers the person difficult or inconvenient.

======================================================================
12. UIP AUTHORITY SOURCES
======================================================================

UIP authority is distributed.

RATEPAYER
    authority = authoritative RP/Vault data

ELECTED MEMBER
    authority = election/governance evidence

STAFF
    authority = appropriate Committee/Subcommittee resolution

PROVIDER
    authority = appropriate Committee/Subcommittee resolution

PUBLIC / ANONYMOUS
    authority = public access rules
    → direct permitted public access

Do not collapse these into "Secretary approves user".

======================================================================
13. UIP RESOLUTION-DRIVEN GOVERNANCE
======================================================================

Resolutions are first-class UIP governance records.

A resolution must ultimately support formal governance information such as:

    resolution number
    resolution date
    meeting date where applicable
    originating Committee/Subcommittee
    title/subject
    decision/resolution text
    proposer/mover where applicable
    seconder where applicable
    voting/result where applicable
    effective date
    ratification required
    ratified date
    ratifying body
    status
    supporting minutes/document/evidence
    captured-by user
    audit timestamps

Exact schema is NOT yet frozen.

The governing requirement is traceability:

    Resolution
        ↓
    authorised decision / appointment
        ↓
    relationship / role
        ↓
    permissions
        ↓
    dashboard

AIT should be capable of answering:

    "By what authority does this person hold this UIP position/access?"

The Secretary/Admin records or implements governance authority.
The Secretary/Admin must not become the source of authority merely through
system access.

======================================================================
14. UIP INITIAL ADMIN AUTHORITY
======================================================================

Secretary and UIP Admin are conceptually different:

    Secretary = governance/office position
    UIP Admin = authority to administer the AIT UIP workspace

They may be held by the same person but are not synonymous.

For first setup:

    Existing UIP
        ↓
    valid UIP governance decision/resolution
        ↓
    named authenticated person authorised to establish/administer AIT presence
        ↓
    initial UIP Admin authority
        ↓
    existing UIP established on AIT

Preferred evidence is a formal UIP resolution expressly authorising the named
person to establish/administer the UIP's AIT workspace.

AIT records the authority; AIT does not create it.

Do not create an ordinary AIT human reviewer as the source of UIP authority.

Missing, contradictory or disputed evidence is an exception to normal
onboarding and must not result in guessed authority.

AIT is not to become a constitutional adjudication system.

======================================================================
15. UIP ROLE-SPECIFIC DESTINATIONS
======================================================================

Once legitimate authority/role is established, users reach the appropriate
working environment.

Examples:

    Chairman / Vice-Chairman
        → UIP Command Centre

    Treasurer
        → Accounts / Finance Dashboard

    other elected members
        → appropriate portfolio/functions

    Staff
        → authorised operational environment

    Provider
        → restricted provider environment

    Ratepayer
        → Ratepayer Dashboard

    Public / anonymous
        → Public Dashboard

Dashboard tiles must NEVER manufacture roles.

This is directly relevant to the existing UIP defect where provider identity
can incorrectly collapse into staff identity.

Correct the authority/membership/role architecture rather than patching a
single tile.

======================================================================
16. RCM COMMERCIAL LIFECYCLE — CURRENT FROZEN DIRECTION
======================================================================

Commercial entitlement belongs to the retirement-home organisation.

Conceptual lifecycle:

    Create home + establish founding authority
        ↓
    Organisation activation
        ↓
    Active RCM service
        ↓
    Expired entitlement: grace period with normal authorised service
        ↓
    Restricted service after grace / commercial suspension
        ↓
    Restoration after reactivation

These are business conditions, not prescribed database statuses.

Pricing/payment does NOT create Graham's authority.

After Graham creates Archoney House, organisation-level activation intervenes
before normal operational use.

Pre-activation recommended boundary:

    Graham may:
        - confirm basic home setup
        - view service/commercial status
        - arrange activation

    Waiting-user approval, role allocation and operational functions remain
    unavailable until organisation activation.

Mary remains in the Waiting Room while AH is inactive.

When AH is commercially active:

    BOTH conditions must hold for a protected function:

        organisation service availability
            AND
        user's independent authority in that home/context

Organisation payment does not create individual membership, role or permission.

Example:

    Mary belongs legitimately to:
        Archoney House — commercially active
        Samaritan Home — commercially inactive

    Mary may use authorised AH contexts.

    Mary's SH relationships/roles remain recorded. If SH is commercially
    restricted after grace or suspended, existing information remains read-only
    within Mary's current authority; no new operational activity is available.

    Mary is NEVER asked to purchase SH access personally.

    AH entitlement does not activate SH.

FROZEN COMMERCIAL-LAPSE POLICY

ACTIVE:
    Full operational service, subject to each user's existing home/relationship/
    role authority.

EXPIRED - GRACE:
    Normal authorised service continues temporarily.
    Owner/Admin receives clear renewal warnings.
    The grace-period duration is not yet selected.

RESTRICTED AFTER GRACE / COMMERCIAL SUSPENSION:
    Existing information remains available read-only to users who are otherwise
    authorised to see it.
    No user gains additional visibility because the organisation is restricted.

    Owner/Admin additionally retains the functions required to:
        - view service/commercial status;
        - reactivate/renew the organisation;
        - perform appropriate organisation-level export.

    No new operational activity is permitted while commercially restricted,
    including new operational records, waiting-user approvals, relationship/role
    changes, care entries, financial transactions or equivalent mutations.

REACTIVATION:
    Normal service availability resumes using CURRENT authority records.
    Do not recreate, restore or alter memberships, relationships, roles or
    permissions merely because payment resumes.

Commercial lapse/suspension must never erase or rewrite organisation data,
people, memberships, relationships, roles, permissions, governance/history or
operational records.

======================================================================
17. COMMERCIAL CONCEPT SEPARATION
======================================================================

Keep these independent:

    Organisation authority
        = who may establish/administer the organisation

    Organisation commercial entitlement
        = whether the organisation has the commercial right to use the program

    User membership/relationship
        = why the person legitimately belongs to the organisation

    User role/permissions
        = what the person is authorised to do

    Service availability
        = what the program currently makes available to the organisation

Payment must never become proof of user authority.

======================================================================
18. COMMERCIAL QUESTIONS NOT YET FROZEN
======================================================================

Do NOT invent answers yet for:

1. Duration of the commercial-expiry grace period. Normal authorised service
   during grace and the subsequent read-only restriction are frozen above.

2. Exact prices, billing periods or packages.

3. Paystack checkout implementation.

4. UIP commercial lifecycle details.

These remain open design questions.

======================================================================
19. IMPLEMENTATION HOLD
======================================================================

This section records governing functional architecture.

Do NOT treat it as authorisation to implement the redesign.

Before implementation:
    continue functional design,
    inspect existing program code/schema against the frozen architecture,
    identify conflicts,
    and make a controlled implementation plan.

Do not silently modify UIP while implementing RCM or vice versa.

Preserve the program-first rule and avoid unrelated platform cleanup.
