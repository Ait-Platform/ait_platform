"""PostgreSQL contact-only sliding-window quota; reserve before SMTP."""
from datetime import timedelta
import hashlib
import hmac
from ipaddress import ip_address
from math import ceil
import os
from uuid import uuid4

from flask import current_app, request
from sqlalchemy import text

from app.extensions import db

LIMIT = 3
WINDOW_SECONDS = 3600


class ContactActionError(Exception):
    def __init__(self, status, message, retry_after=None):
        self.status, self.message, self.retry_after = status, message, retry_after


def client_ip():
    # Render's public ingress passes through Cloudflare, which supplies this header.
    # Never trust it for local/direct traffic, and never take the leftmost XFF value.
    if os.getenv('RENDER') == 'true':
        value = request.headers.get('CF-Connecting-IP', '')
    else:
        value = request.remote_addr or ''
    try:
        if '%' in value:
            raise ValueError()
        address = ip_address(value.strip())
        if address.version == 6 and address.ipv4_mapped:
            address = address.ipv4_mapped
        return str(address)
    except ValueError:
        raise ContactActionError(503, 'The contact form is temporarily unavailable. Please try later.')


def reserve_contact_attempt():
    secret = current_app.secret_key
    if not secret:
        raise ContactActionError(503, 'The contact form is temporarily unavailable. Please try later.')
    key = secret if isinstance(secret, bytes) else secret.encode('utf-8')
    digest = hmac.new(key, ('contact-email-v1:'+client_ip()).encode(), hashlib.sha256).hexdigest()
    # Same IP shares one transaction lock across processes/instances. SMTP happens after commit.
    lock = int.from_bytes(bytes.fromhex(digest[:16]), 'big', signed=True)
    try:
        db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': lock})
        now = db.session.execute(text('SELECT clock_timestamp()')).scalar_one()
        count, oldest = db.session.execute(text("""
            SELECT count(*), min(attempted_at) FROM contact_email_attempt
            WHERE ip_hash=:ip AND attempted_at>:cutoff
        """), {'ip': digest, 'cutoff': now-timedelta(seconds=WINDOW_SECONDS)}).one()
        if count >= LIMIT:
            retry = max(1, ceil((oldest+timedelta(seconds=WINDOW_SECONDS)-now).total_seconds()))
            raise ContactActionError(429, 'Too many contact submissions. Please try again later.', retry)
        db.session.execute(text("""
            INSERT INTO contact_email_attempt (id,ip_hash,attempted_at) VALUES (:id,:ip,:now)
        """), {'id': uuid4().hex, 'ip': digest, 'now': now})
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
