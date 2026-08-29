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
  - *CRITICAL RULE:* AI scripts run locally. Any configuration rows (like new auth_subject rows) inserted by the AI will ONLY exist locally. You MUST provide the raw psql statements to the Admin so they can manually execute them on the Render database!
- The connection is managed via the DATABASE_URL environment variable.

## 3. Persistent Disk vs Static Assets
- Static Assets: Any files required for the app to run (like SACE workshop slides, logos, CSS, JavaScript) must be placed in app/static/ and pushed via Git.
- Dynamic Uploads: Files uploaded by users (e.g., SPV files, CSVs, profile pictures) are saved to Render's Persistent Disk. These are completely separate from the codebase and should never be checked into version control.

## 4. Local Project Path
- The canonical local development directory for this project on the primary Windows machine is: D:\Users\yeshk\Documents\ait_platform

## 5. System Architecture (SACE Module)
- **Facilitator-Participant Sync:** The SACE workshop operates on a strict singleton room design (demo-session-1).
- **State Management:** The Facilitator Dashboard dictates the state (slides, lobby, active). The Participant App strictly polls /sace/workshop/get_state every 1 second.
- **Interaction Logging:** All participant votes (polls, reflections) are securely UPSERTED/INSERTED into the Postgres SaceWorkshopInteraction table. This creates a permanent, crash-proof audit trail for the SACE evaluators.
- **Routing Rules:** 
  - The Participant Check-In is completely separated into /sace/participant/join.
  - The Interactive App (/sace/workshop/interactive) contains NO lobby HTML. If a user hits it without joining, they are bounced back to the join route.
- **Render Deployment Constraint:** Render takes 5-9 minutes to deploy. **Code Freezes** are required before live SACE evaluation demos. 

