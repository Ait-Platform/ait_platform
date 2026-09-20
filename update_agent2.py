with open("C:/Users/Sanjith/.gemini/antigravity/brain/62204572-bbdc-4628-9905-cc45ecf51188/agent.md", "r", encoding="utf-8") as f:
    text = f.read()

import re
old_text = r"""\*   \*\*Infrastructure \(Cloudflare R2 Migrations\):\*\* The core R2 integration and `boto3` pipeline is complete. The following modules are earmarked to be migrated to Cloudflare R2 storage in the future:
    1. \*\*Platform Branding:\*\* User profile avatars, UIP/Community logos, and custom UI banners.
    2. \*\*Governance Archives:\*\* AGM Minutes \(PDFs\), Constitutions, Bylaws, and Founding Declarations.
    3. \*\*Financial Records:\*\* Audit reports, spreadsheets, and vendor/contractor invoices for the Treasurer's dashboard.
    4. \*\*Intake Desk Verification:\*\* Proof of Residence \(utility bills, IDs\) uploaded by residents during the onboarding queue.
    5. \*\*Public Communication:\*\* Media attachments for community newsletters, mass emails, and public notices."""

new_text = """*   **Infrastructure (Cloudflare R2 Migrations - PLATFORM WIDE):** The core R2 integration and `boto3` pipeline is complete. To maintain focus on the UIP build and protect live production environments, **ALL migrations of existing platform slugs to R2 are deferred to a future dedicated infrastructure phase.** The master migration list includes:
    1. **UIP Slug:** Organogram avatars, AGM Minutes, financial audits, proof-of-residence uploads, and community notices.
    2. **Reading Slug:** The 18+ educational videos, student assessments, and reading materials.
    3. **Home Slug:** All static site assets, marketing imagery, and homepage graphics.
    4. **Protrade Slug:** Video disks, company logos, and trading/vendor assets.
    *(Note: Do not touch live slugs until explicitly instructed, to avoid triggering massive regression testing).*"""

text = re.sub(old_text, new_text, text)

with open("C:/Users/Sanjith/.gemini/antigravity/brain/62204572-bbdc-4628-9905-cc45ecf51188/agent.md", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated agent.md with global R2 migrations list")
