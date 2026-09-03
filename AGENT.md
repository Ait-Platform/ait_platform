# Antigravity Rules for AIT Platform

## 1. Flask Templates (Critical)
- The base template for this project is named layout.html. 
- NEVER use base.html. 
- ALWAYS use layout.html when creating new templates or modifying existing ones.

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
- Dynamic Uploads: Files uploaded by users (e.g., SPV files, CSVs, profile pictures) are saved to Render's Persistent Disk. These are completely separate from the codebase and should never be checked into version control.

## 4. Local Project Path
- The canonical local development directory for this project on the primary Windows machine is: D:\Users\yeshk\Documents\ait_platform

## 5. System Architecture (SACE Module)
- **R Page (Reneilwe / SACE Admin Page):** We will design an "R page" for the SACE Admin later. This page will host the Audit Logs (/sace/audit_report). Do not place the Audit Logs button on the participant or evaluator hubs.
- **Provider vs SACE Relationship (Critical Domain Context):** AIT is the *Provider*. We are giving SACE an activity for approval. The activities belong to the Provider (AIT), NOT SACE. SACE is merely an endorsement entity. When naming pages or UI elements, do not frame them as if SACE owns the activity (e.g., use "Provider's SACE Activities" rather than "SACE\'s Evaluation Hub").
- **Facilitator-Participant Sync:** The SACE workshop operates on a strict singleton room design (demo-session-1).
- **State Management:** The Facilitator Dashboard dictates the state (slides, lobby, active). The Participant App strictly polls /sace/workshop/get_state every 1 second.
- **Interaction Logging:** All participant votes (polls, reflections) are securely UPSERTED/INSERTED into the Postgres SaceWorkshopInteraction table. This creates a permanent, crash-proof audit trail for the SACE evaluators.
- **Routing Rules:** 
  - The Participant Check-In is completely separated into /sace/participant/join.
  - The Interactive App (/sace/workshop/interactive) contains NO lobby HTML. If a user hits it without joining, they are bounced back to the join route.
- **Render Deployment Constraint:** Render takes 5-9 minutes to deploy. **Code Freezes** are required before live SACE evaluation demos.
  - **Communication Note:** The user uses the acronym **"wew"** (While We Wait). This means we are waiting 4-9 minutes for Render to deploy. During a 'wew' phase, we look forward and plan what to do next without writing any code. 



## 6. Payment Portal
- The official payment portal for the platform is **Paystack**, NOT Yoco.
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
