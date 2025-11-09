# app/mail_utils.py
from flask_mail import Message
from . import mail
from typing import Iterable, Optional, Sequence, Tuple, Union
from flask import current_app, render_template
from flask_mail import Message
from app.extensions import mail  # Mail() lives here

def send_pdf_email(to_email: str, rid: int, pdf_bytes: bytes) -> None:
    msg = Message(
        subject=f"Your Loss Assessment Report (Run {rid})",
        recipients=[to_email],
        body="Attached is your Loss Assessment Report.\n\nKeep this for your records.",
    )
    msg.attach(
        filename=f"loss-report-run{rid}.pdf",
        content_type="application/pdf",
        data=pdf_bytes,
    )
    mail.send(msg)

# app/utils/mailer.py


Attachment = Tuple[str, str, bytes]  # (filename, mimetype, raw_bytes)

def send_mail(
    subject: str,
    recipients: Union[str, Sequence[str]],
    *,
    body: Optional[str] = None,
    html: Optional[str] = None,
    attachments: Optional[Iterable[Attachment]] = None,
    sender: Optional[str] = None,
    cc: Optional[Sequence[str]] = None,
    bcc: Optional[Sequence[str]] = None,
) -> None:
    recips = [recipients] if isinstance(recipients, str) else list(recipients or [])
    msg = Message(
        subject=subject,
        sender=sender or current_app.config.get("MAIL_DEFAULT_SENDER"),
        recipients=recips,
        cc=list(cc or []),
        bcc=list(bcc or []),
    )
    if body:
        msg.body = body
    if html:
        msg.html = html
    for (fn, ctype, data) in (attachments or []):
        msg.attach(fn, ctype, data)
    mail.send(msg)
