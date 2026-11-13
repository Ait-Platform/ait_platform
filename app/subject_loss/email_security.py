"""Guards shared only by the two legacy LOSS report email actions."""
from functools import wraps
from uuid import uuid4

from email_validator import EmailNotValidError, validate_email
from flask import current_app, jsonify
from flask_login import current_user
from sqlalchemy import text

from app.extensions import db

COOLDOWN_SECONDS = 600
LOCK_NAMESPACE = 1279607635


class EmailActionError(Exception):
    def __init__(self, status, message, retry_after=None):
        self.status, self.message, self.retry_after = status, message, retry_after


def controlled_email_errors(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        try:
            return view(*args, **kwargs)
        except EmailActionError as exc:
            db.session.rollback()
            response = jsonify(error=exc.message)
            if exc.retry_after:
                response.headers['Retry-After'] = str(exc.retry_after)
            return response, exc.status
        except Exception as exc:
            db.session.rollback()
            # Do not reflect SMTP/PDF/database errors or recipient details.
            current_app.logger.error('LOSS email action failed (%s)', type(exc).__name__)
            return jsonify(error='The report could not be emailed. Please try later or contact support.'), 503
    return wrapped


def authorised_report(run_id):
    try:
        run_id = int(run_id)
        if run_id <= 0:
            raise ValueError()
    except (ValueError, TypeError):
        raise EmailActionError(400, 'Select a valid report.')
    if not current_user.is_authenticated:
        raise EmailActionError(401, 'Please sign in to email a report.')
    actor = db.session.execute(text(
        'SELECT id, email FROM "user" WHERE id=:id AND is_active=1'),
        {'id': current_user.id}).mappings().first()
    report = db.session.execute(text("""
        SELECT r.id AS run_id, u.id AS owner_id, u.email
        FROM lca_run r JOIN "user" u ON u.id=r.user_id
        WHERE r.id=:id AND u.is_active=1
    """), {'id': run_id}).mappings().first()
    if not actor or not report:
        raise EmailActionError(404, 'Report unavailable.')
    if report['owner_id'] != actor['id']:
        # Recheck an explicit persisted global/LOSS-admin grant, never a referrer or session role.
        admin = db.session.execute(text("""
            SELECT 1 FROM auth_approved_admin
            WHERE lower(email)=lower(:email)
              AND lower(CAST(active AS text)) IN ('true','t','1')
            UNION ALL
            SELECT 1 FROM auth_subject_admin a JOIN auth_subject s ON s.id=a.subject_id
            WHERE lower(a.email)=lower(:email) AND s.slug='loss' AND s.is_active=1
            LIMIT 1
        """), {'email': actor['email']}).first()
        if not admin:
            raise EmailActionError(404, 'Report unavailable.')
    inconsistent = db.session.execute(text("""
        SELECT EXISTS(SELECT 1 FROM lca_result WHERE run_id=:run AND user_id<>:owner)
            OR EXISTS(SELECT 1 FROM lca_response WHERE run_id=:run AND user_id<>:owner)
    """), {'run': run_id, 'owner': report['owner_id']}).scalar()
    if inconsistent:
        raise EmailActionError(404, 'Report unavailable.')
    try:
        recipient = validate_email(report['email'] or '', check_deliverability=False).normalized
    except EmailNotValidError:
        raise EmailActionError(400, 'The report owner needs a valid saved account email address.')
    return dict(run_id=run_id, owner_id=report['owner_id'], recipient=recipient)


def guarded_send(report, pdf_bytes, sender):
    """Reserve durably before SMTP; serialize both routes by report owner."""
    if not isinstance(pdf_bytes, bytes) or not pdf_bytes.startswith(b'%PDF-'):
        raise EmailActionError(503, 'The report PDF could not be generated. No email was attempted.')
    locked = db.session.execute(text('SELECT pg_try_advisory_xact_lock(:namespace,:owner)'),
        {'namespace': LOCK_NAMESPACE, 'owner': report['owner_id']}).scalar()
    if not locked:
        raise EmailActionError(409, 'A report email is already being processed. Please wait.')
    if authorised_report(report['run_id']) != report:
        raise EmailActionError(409, 'The report or account changed. Please reopen the report.')
    previous = db.session.execute(text(
        'SELECT status FROM loss_report_email_send WHERE run_id=:run FOR UPDATE'),
        {'run': report['run_id']}).mappings().first()
    if previous and previous['status'] in ('attempted', 'uncertain'):
        raise EmailActionError(409, 'A previous delivery is unconfirmed. Contact support before sending again.')
    recent = db.session.execute(text("""
        SELECT 1 FROM loss_report_email_send WHERE owner_id=:owner
        AND attempted_at > clock_timestamp() - (:seconds * INTERVAL '1 second') LIMIT 1
    """), {'owner': report['owner_id'], 'seconds': COOLDOWN_SECONDS}).first()
    if recent:
        raise EmailActionError(429, 'A report email was recently submitted. Please wait 10 minutes before sending again.', COOLDOWN_SECONDS)
    attempt = uuid4().hex
    db.session.execute(text("""
        INSERT INTO loss_report_email_send
            (run_id,owner_id,actor_id,recipient,attempt_id,status,attempted_at)
        VALUES (:run,:owner,:actor,:recipient,:attempt,'attempted',clock_timestamp())
        ON CONFLICT (run_id) DO UPDATE SET owner_id=EXCLUDED.owner_id,
            actor_id=EXCLUDED.actor_id,recipient=EXCLUDED.recipient,
            attempt_id=EXCLUDED.attempt_id,status='attempted',attempted_at=EXCLUDED.attempted_at
    """), dict(run=report['run_id'], owner=report['owner_id'], actor=current_user.id,
                   recipient=report['recipient'], attempt=attempt))
    db.session.commit()
    try:
        transport = current_app.extensions.get('mail')
        if current_app.config.get('MAIL_SUPPRESS_SEND') or getattr(transport, 'suppress', False):
            raise RuntimeError('Mail suppressed')
        if sender() is not True:
            raise RuntimeError('Transport acceptance unconfirmed')
    except Exception:
        db.session.rollback()
        try:
            db.session.execute(text("""
                UPDATE loss_report_email_send SET status='uncertain'
                WHERE run_id=:run AND attempt_id=:attempt
            """), {'run': report['run_id'], 'attempt': attempt})
            db.session.commit()
        except Exception:
            db.session.rollback()
        raise EmailActionError(503, 'Delivery was not confirmed. Contact support before sending again.')
    db.session.execute(text("""
        UPDATE loss_report_email_send SET status='accepted'
        WHERE run_id=:run AND attempt_id=:attempt
    """), {'run': report['run_id'], 'attempt': attempt})
    db.session.commit()
