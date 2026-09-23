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
  - *CRITICAL RULE (UPDATED):* The agent is authorized to use the Render connection string (if available in .env or context) to run Python SQL scripts directly against the live remote database when executing migrations, injecting pricing data, or making structural changes. Always ensure both local and remote DBs are kept in sync.
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

AIT has historical maintenance/diagnostic scripts containing old or hard-coded PostgreSQL usernames and connection details. These are NOT authoritative credentials.

Known obsolete production username: `ait_platform_db_user`.
Current Render PostgreSQL username: `aitplatformdb_7r78_user`.

- NEVER obtain production database credentials from maintenance scripts, diagnostic scripts, scratch files, old code, Git history or hard-coded connection strings.
- NEVER assume a credential found in source code is current.
- Local `.env` is intentionally LOCAL and its `DATABASE_URL` points to `ait_local_db`. Do not replace it with the Render production URL.
- For LOCAL database work, use the current local `.env` / explicitly verified `ait_local_db`.
- For PRODUCTION database work, the source of truth is the current Render PostgreSQL resource configuration.
- When external production access is required, use the current **Render External Database URL** obtained from Render > PostgreSQL resource > Connect > External.
- When running inside Render, use the production connection configuration assigned to the running service rather than an old hard-coded URL.
- Before ANY production database write, independently verify `current_database()` and `current_user`.
- Production credentials must not be printed into reports, logs, prompts or committed files.
- Do not permanently store the Render production `DATABASE_URL` in the local `.env`.
- If the authoritative production credential is unavailable, STOP and request/access the current Render configuration. Do not fall back to credentials discovered in source files.

Reason: During Retirement deployment preflight, an obsolete hard-coded maintenance-script username `ait_platform_db_user` was used and PostgreSQL returned `role is not permitted to log in`. The current Render database uses `aitplatformdb_7r78_user`. This was a credential-source mistake, not a Retirement application failure.

**PARKED LEGACY CREDENTIAL REFERENCES:** Maintenance, diagnostic and scratch scripts still contain obsolete connection references. They are not authoritative and must not be used as credential sources. Leave their cleanup to a separately scoped task.


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
