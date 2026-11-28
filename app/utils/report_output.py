"""Reusable current-report preview and shared email dispatch helpers.
Programme adapters supply data, authorisation and durable reservation callbacks.
"""
import hashlib
import json
from uuid import uuid4
from flask import abort, current_app
from itsdangerous import URLSafeTimedSerializer, BadSignature
from email_validator import validate_email, EmailNotValidError
from app.utils.mailer import send_standard_report_email


def digest(data):
    return hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def preview_token(programme, user_id, data, generated):
    signer=URLSafeTimedSerializer(current_app.secret_key,salt='ait-output-'+programme)
    return signer.dumps(dict(user=user_id,digest=digest(data),generated=generated,nonce=uuid4().hex))


def verify_preview(programme, user_id, data, token):
    signer=URLSafeTimedSerializer(current_app.secret_key,salt='ait-output-'+programme)
    try:claim=signer.loads(token or '',max_age=1800)
    except BadSignature:abort(400,description='This preview is invalid or expired. Click Preview again.')
    if claim.get('user')!=user_id:abort(403,description='This preview belongs to another account.')
    if claim.get('digest')!=digest(data):abort(409,description='The report figures or profile changed. Click Preview again before exporting or emailing.')
    return claim


def send_report_once(*, fields, pdf, filename, label, reserve, finish):
    """Reserve durably BEFORE SMTP; failed/uncertain attempts cannot auto-resend."""
    try:recipient=validate_email(fields.get('to_email',''),check_deliverability=False).normalized
    except EmailNotValidError:abort(400,description='Enter a valid recipient email address.')
    subject=fields.get('subject','').strip();body=fields.get('body_text','').strip()
    if len(recipient)>254 or not subject or len(subject)>200 or any(c in subject for c in '\r\n') or not body or len(body)>5000:
        abort(400,description='Enter a subject up to 200 characters and a message up to 5000 characters.')
    if not pdf.startswith(b'%PDF-') or len(pdf)>10*1024*1024:
        abort(503,description='The report PDF is unavailable or exceeds the email attachment limit. No email was attempted.')
    reserve()
    try:
        transport=current_app.extensions.get('mail')
        if current_app.config.get('MAIL_SUPPRESS_SEND') or getattr(transport,'suppress',False):raise RuntimeError('Mail suppressed')
        if not send_standard_report_email(to_email=recipient,subject=subject,pdf_url='',pdf_bytes=pdf,filename=filename,message=body,document_label=label):
            raise RuntimeError('Acceptance not confirmed')
    except Exception:
        finish('uncertain')
        abort(503,description='Email delivery was not confirmed. Automatic retry is blocked to prevent duplicates. Check delivery before creating a new preview.')
    finish('accepted')
