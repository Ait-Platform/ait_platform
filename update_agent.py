with open("C:/Users/Sanjith/.gemini/antigravity/brain/62204572-bbdc-4628-9905-cc45ecf51188/agent.md", "r", encoding="utf-8") as f:
    text = f.read()

import re
old_text = r"""\*   \*\*Infrastructure \(Cloudflare R2\):\*\* The platform currently uses external URLs for MVP image uploads \(like the Organogram\). In the future infrastructure polish phase, all file and image uploads will be migrated to \*\*Cloudflare R2\*\* Object Storage. \(Note: The codebase already relies on Cloudflare headers like `CF-IPCountry` for routing, so R2 integrates perfectly into the existing ecosystem\)."""

new_text = """*   **Infrastructure (Cloudflare R2 Migrations):** The core R2 integration and `boto3` pipeline is complete. The following modules are earmarked to be migrated to Cloudflare R2 storage in the future:
    1. **Platform Branding:** User profile avatars, UIP/Community logos, and custom UI banners.
    2. **Governance Archives:** AGM Minutes (PDFs), Constitutions, Bylaws, and Founding Declarations.
    3. **Financial Records:** Audit reports, spreadsheets, and vendor/contractor invoices for the Treasurer's dashboard.
    4. **Intake Desk Verification:** Proof of Residence (utility bills, IDs) uploaded by residents during the onboarding queue.
    5. **Public Communication:** Media attachments for community newsletters, mass emails, and public notices."""

text = re.sub(old_text, new_text, text)

with open("C:/Users/Sanjith/.gemini/antigravity/brain/62204572-bbdc-4628-9905-cc45ecf51188/agent.md", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated agent.md with R2 migrations list")
