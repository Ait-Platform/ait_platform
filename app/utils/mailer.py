# app/utils/mailer.py
from flask_mail import Message
from flask import current_app
from app.extensions import mail
import logging
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

def send_pdf_email(to_email: str, subject: str, body_text: str, pdf_bytes: bytes, filename: str = "report.pdf"):
    """
    Sends an email with a single PDF attachment using Flask-Mail.
    Requires MAIL_* settings in config.py to be valid.
    """
    if not to_email:
        raise ValueError("to_email is required")

    msg = Message(
        subject=subject or "Your PDF",
        recipients=[to_email],
        body=body_text or "",
    )

    # Attach PDF
    if pdf_bytes:
        msg.attach(
            filename or "report.pdf",
            "application/pdf",
            pdf_bytes,
        )

    # Respect MAIL_DEFAULT_SENDER if set (Flask-Mail uses it automatically if not overridden)
    mail.send(msg)
    return True

# app/utils/emailer.py


log = logging.getLogger(__name__)


def send_email(subject: str, recipients: list[str], body: str, html: str | None = None):
    try:
        msg = Message(subject=subject, recipients=recipients)
        # enforce visible From (alias) while authenticating with MAIL_USERNAME
        msg.sender = current_app.config.get("MAIL_DEFAULT_SENDER")
        msg.body = body
        if html:
            msg.html = html
        mail.send(msg)
        current_app.logger.info("send_email: sent '%s' to %s", subject, recipients)
        return True
    except Exception as e:
        current_app.logger.exception("send_email: failed '%s' to %s: %s", subject, recipients, e)
        return False


'''
def send_loss_report_email(to, run_id, user_id, pdf_url):
    """Send learner report email (used by admin + learner finish)."""
    try:
        msg = f"Your report is ready. You can view it here: {pdf_url}"
        mail.send_message(
            subject=f"Loss Report #{run_id}",
            recipients=[to],
            body=msg,
        )
        current_app.logger.info(f"Email sent to {to}")
    except Exception as e:
        current_app.logger.exception(f"Email send failed for {to}: {e}")
'''


def send_loss_report_email(*, to: str, run_id: int, user_id: int, pdf_url: str, learner_name: str | None = None) -> None:
    """
    Sends the learner's report email with BOTH a link and the PDF attached.
    Requires Flask-Mail to be configured as current_app.extensions["mail"].
    """
    mail = current_app.extensions.get("mail")
    if mail is None:
        current_app.logger.warning("Flask-Mail not configured; skipping email send.")
        return

    subject = f"Your LOSS Assessment Report (Run #{run_id})"
    greeting = f"Hi {learner_name}," if learner_name else "Hi,"
    body = (
        f"{greeting}\n\n"
        "Your LOSS assessment report is ready.\n\n"
        f"Open it here:\n{pdf_url}\n\n"
        "We’ve also attached the PDF in case links are blocked.\n\n"
        "If you didn’t request this, please ignore this email.\n\n"
        "— AIT Platform"
    )

    msg = Message(subject=subject, recipients=[to], body=body)

    # Attach PDF bytes (fetch from our own absolute URL)
    pdf_bytes = None
    try:
        with urlopen(pdf_url, timeout=20) as resp:
            if resp.status == 200:
                pdf_bytes = resp.read()
            else:
                current_app.logger.warning("PDF fetch returned status %s for %s", resp.status, pdf_url)
    except (HTTPError, URLError) as e:
        current_app.logger.exception("Failed to fetch PDF for attachment: %s", e)
    except Exception as e:
        current_app.logger.exception("Unexpected error fetching PDF: %s", e)

    if pdf_bytes:
        filename = f"LOSS-Report-Run-{run_id}.pdf"
        msg.attach(filename, "application/pdf", pdf_bytes)
    else:
        current_app.logger.warning("No PDF bytes; sending email without attachment for run=%s", run_id)

    # Send
    mail.send(msg)
    current_app.logger.info("Report email sent to %s (run=%s user=%s)", to, run_id, user_id)
