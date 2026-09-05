import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

sace_cert_gen_func = '''
def _generate_sace_certificate_pdf(certificate_id, learner_name, completed_at, user_id=None):
    from flask import current_app, render_template
    from datetime import datetime
    from app.pdf.generator import generate_pdf_from_html
    
    if isinstance(completed_at, str):
        try:
            completed_at = datetime.fromisoformat(completed_at)
        except Exception:
            completed_at = datetime.utcnow()
    elif completed_at is None:
        completed_at = datetime.utcnow()

    completed_date = completed_at.strftime("%d %B %Y")

    try:
        html_out = render_template(
            "program_sace/post_test/certificate_pdf.html",
            learner_name=learner_name,
            completed_date=completed_date,
            certificate_id=certificate_id,
        )
        pdf_bytes = generate_pdf_from_html(html_out)
        return pdf_bytes
    except Exception as e:
        current_app.logger.error(f"SACE PDF generation failed for {certificate_id}: {e}")
        return b""
'''

# We will inject this before the email_certificate function, and update the import in email_certificate.
# In email_certificate, we change rom app.subject_reading.routes import _generate_certificate_pdf, _email_certificate_pdf
# to rom app.subject_reading.routes import _email_certificate_pdf
# and we call _generate_sace_certificate_pdf instead.

text = text.replace(
    'from app.subject_reading.routes import _generate_certificate_pdf, _email_certificate_pdf',
    'from app.subject_reading.routes import _email_certificate_pdf'
)
text = text.replace(
    '_generate_certificate_pdf(',
    '_generate_sace_certificate_pdf('
)

text += "\n" + sace_cert_gen_func

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
