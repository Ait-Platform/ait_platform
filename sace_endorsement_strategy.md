# SACE ENDORSEMENT STRATEGY (CRITICAL DIRECTIVE)

## The Prime Directive
The **SACE Endorsement** is the absolute gateway for the entire platform. If the SACE Auditor does not endorse the activity, it cannot be sold as a SACE-approved program. 

Therefore, the **SACE Simulator (Auditor View)** is the highest priority feature, and it must be **100% UNBREAKABLE**.

## The "Simple Coding" Rules for the Simulator
1. **NO IFRAMES:** The Simulator must never use <iframe> to load live routes.
2. **NO LIVE DATABASE:** The Simulator must never poll the live database or wait for real teachers to log in.
3. **ONE SINGLE PAGE:** The entire Simulator (simulator.html) must be a self-contained HTML file. All Facilitator (F) and Participant (P) screens are simply hardcoded <div> blocks that are hidden/shown using basic Javascript.
4. **THE TRAFFIC ROBOT:** The Auditor is guided by a "Traffic Robot" system using Red and Green lights on the tabs.
   - **Green Light:** Tells the Auditor "Look here and click next."
   - **Red Light:** Means "Stop / Wait".
5. **THE WORKFLOW:**
   - Auditor reads Guide (Tab A), clicks "Launch Activity".
   - System switches to Tab F (Light goes GREEN). Auditor sees Facilitator slide and clicks "Send to Participant".
   - System switches to Tab P (Light goes GREEN, F goes RED). Auditor sees the Participant screen and interacts with it (e.g., answers a poll).
   - Auditor clicks "Next Activity", routing them back to Tab F (Green).

## Long-Term Separation
The complex, live-database logic required for *real paid users* in a live center is completely separate from the Endorsement Simulator. If the live tech fails during a real show, we can fall back to a physical PowerPoint presentation. But the Endorsement Simulator must never fail.
