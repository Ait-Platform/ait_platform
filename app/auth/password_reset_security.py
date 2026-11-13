"""Password-reset-only distributed quotas and single-attempt background delivery."""
from datetime import timedelta
import hashlib
import hmac
from ipaddress import ip_address
from math import ceil
import os
from threading import Thread
from uuid import uuid4

from email_validator import EmailNotValidError, validate_email
from flask import current_app, render_template, request
from sqlalchemy import text

from app.extensions import db
from app.utils.mailer import send_email
from app.utils.password_reset import make_reset_token

IP_LIMIT = 3
IP_WINDOW_SECONDS = 3600
ACCOUNT_COOLDOWN_SECONDS = 600
ACCOUNT_LOCK_NAMESPACE = 1347572564
URL_TOKEN_MARKER = '__AIT_PASSWORD_RESET_TOKEN__'
GENERIC_MESSAGE = 'If an account matches that email, a password-reset link will be sent when eligible.'


class ResetRequestError(Exception):
    def __init__(self, status, retry_after=None):
        self.status, self.retry_after = status, retry_after


def client_ip():
    # Local to reset: trust Cloudflare's single-address header only on Render ingress.
    value = request.headers.get('CF-Connecting-IP', '') if os.getenv('RENDER') == 'true' else request.remote_addr or ''
    try:
        if '%' in value:
            raise ValueError()
        address = ip_address(value.strip())
        if address.version == 6 and address.ipv4_mapped:
            address = address.ipv4_mapped
        return str(address)
    except ValueError:
        raise ResetRequestError(503)


def reserve_request():
    secret = current_app.secret_key
    if not secret:
        raise ResetRequestError(503)
    key = secret if isinstance(secret, bytes) else secret.encode('utf-8')
    digest = hmac.new(key, ('password-reset-ip-v1:'+client_ip()).encode(), hashlib.sha256).hexdigest()
    lock = int.from_bytes(bytes.fromhex(digest[:16]), 'big', signed=True)
    try:
        db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': lock})
        now = db.session.execute(text('SELECT clock_timestamp()')).scalar_one()
        count, oldest = db.session.execute(text("""
            SELECT count(*),min(attempted_at) FROM password_reset_request
            WHERE ip_hash=:ip AND attempted_at>:cutoff
        """), {'ip': digest, 'cutoff': now-timedelta(seconds=IP_WINDOW_SECONDS)}).one()
        if count >= IP_LIMIT:
            retry = max(1, ceil((oldest+timedelta(seconds=IP_WINDOW_SECONDS)-now).total_seconds()))
            raise ResetRequestError(429, retry)
        db.session.execute(text("""
            INSERT INTO password_reset_request (id,ip_hash,attempted_at) VALUES (:id,:ip,:now)
        """), {'id': uuid4().hex, 'ip': digest, 'now': now})
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise


def reserve_account(email):
    # Exact account matching is case-insensitive, and ambiguous legacy accounts fail closed.
    users = db.session.execute(text("""
        SELECT id,name,email FROM "user" WHERE lower(email)=:email LIMIT 2
    """), {'email': email}).mappings().all()
    if len(users) != 1:
        db.session.rollback()
        return None
    user = dict(users[0])
    db.session.execute(text('SELECT pg_advisory_xact_lock(:namespace,:account)'),
        {'namespace': ACCOUNT_LOCK_NAMESPACE, 'account': user['id']})
    now = db.session.execute(text('SELECT clock_timestamp()')).scalar_one()
    previous = db.session.execute(text("""
        SELECT attempted_at FROM password_reset_send WHERE user_id=:account FOR UPDATE
    """), {'account': user['id']}).scalar()
    if previous and previous > now-timedelta(seconds=ACCOUNT_COOLDOWN_SECONDS):
        db.session.rollback()
        return None
    attempt = uuid4().hex
    db.session.execute(text("""
        INSERT INTO password_reset_send (user_id,attempt_id,attempted_at,status)
        VALUES (:account,:attempt,:now,'attempted')
        ON CONFLICT (user_id) DO UPDATE SET attempt_id=EXCLUDED.attempt_id,
            attempted_at=EXCLUDED.attempted_at,status='attempted'
    """), {'account': user['id'], 'attempt': attempt, 'now': now})
    db.session.commit()
    return user, attempt


def record_outcome(user_id, attempt, status):
    db.session.execute(text("""
        UPDATE password_reset_send SET status=:status
        WHERE user_id=:account AND attempt_id=:attempt
    """), {'status': status, 'account': user_id, 'attempt': attempt})
    db.session.commit()


def deliver_once(email, reset_url_pattern):
    """Called in a background app context. Never retries or refunds a reservation."""
    reservation = None
    try:
        # All account-dependent work stays outside the HTTP request path.
        if len(email) > 320:
            return
        try:
            validate_email(email, check_deliverability=False)
        except EmailNotValidError:
            return
        reservation = reserve_account(email)
        if reservation is None:
            return
        user, attempt = reservation
        current_email = db.session.execute(text('SELECT email FROM "user" WHERE id=:id'), {'id': user['id']}).scalar()
        if current_email != user['email']:
            raise RuntimeError('Account changed')
        transport = current_app.extensions.get('mail')
        if current_app.config.get('MAIL_SUPPRESS_SEND') or getattr(transport, 'suppress', False):
            raise RuntimeError('Mail suppressed')
        token = make_reset_token(user['id'])
        reset_url = reset_url_pattern.replace(URL_TOKEN_MARKER, token)
        html = render_template('auth/email_reset.html', reset_url=reset_url, user=user)
        accepted = send_email('Reset your password', [user['email']], body=f'Reset link: {reset_url}', html=html)
        if accepted is not True:
            raise RuntimeError('Transport acceptance unconfirmed')
        record_outcome(user['id'], attempt, 'accepted')
    except Exception as exc:
        db.session.rollback()
        if reservation is not None:
            user, attempt = reservation
            try:
                record_outcome(user['id'], attempt, 'uncertain')
            except Exception:
                db.session.rollback()
        # No submitted address, token, SMTP error text or connection string in this log.
        current_app.logger.error('Password reset delivery unconfirmed (%s)', type(exc).__name__)


def start_delivery(email, reset_url_pattern):
    app = current_app._get_current_object()
    def work():
        with app.app_context():
            deliver_once(email, reset_url_pattern)
    # One best-effort task per accepted request, including unknown addresses.
    # Worker shutdown can lose an in-flight task; no automatic retry is introduced.
    Thread(target=work, name='password-reset-email', daemon=True).start()
