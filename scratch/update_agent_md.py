import re

file_path = 'AGENT.md'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Update Rule 9
old_rule9 = '''## 9. Certificates & Post-Test
- **NEVER** build custom HTML certificates for download.
- **ALWAYS** use the standardized _generate_certificate_pdf and _email_certificate_pdf functions (e.g., from  pp.subject_reading.routes) to generate the official AIT PDF and email it to the user.
- **Email Delivery:** Instead of direct download buttons, provide a form where the user can confirm/enter their email address to have the certificate sent to them.'''

new_rule9 = '''## 9. Standardised Document & Email Services (Critical)
- **NEVER** build custom HTML certificates for download or use alternative PDF libraries.
- **Certificate Generator:** Always use pp.utils.pdf_render.html_to_pdf_bytes. 
  - *Note:* This strictly relies on WeasyPrint for Render (Linux) compatibility and wkhtmltopdf for local Windows testing. NEVER inject xhtml2pdf as a fallback; it destroys Tailwind CSS layouts (flexbox/grid) and crashes on base64 images causing silent failures.
- **Standardised Email Sender:** Always use pp.utils.mailer.send_pdf_email to deliver attachments. 
- **Pattern:** Follow the pattern established in pp.subject_reading.routes (_generate_certificate_pdf and _email_certificate_pdf). This ensures the exact same PDF engine and mailer are used uniformly so you do not have to reinvent or re-explain these parts.'''

if '## 9. Standardised Document' not in text:
    text = text.replace(old_rule9, new_rule9)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
