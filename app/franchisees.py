"""Small registration workflow following SACE's hashed, expiring invitation pattern."""
import base64
import hashlib
import hmac
import re
import secrets
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet, InvalidToken
from email_validator import validate_email, EmailNotValidError
from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user
from flask_mail import Message
from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError

from app.extensions import db, mail
from app.models.auth import User
from app.models.franchisee import FranchiseeRegistration as Registration
from app.utils.roles import is_admin

franchisee_bp = Blueprint('franchisee_bp', __name__)


@franchisee_bp.context_processor
def local_dates():
    def display(value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return value.astimezone(ZoneInfo('Africa/Johannesburg')).strftime('%Y-%m-%d %H:%M SAST')
    return {'franchisee_date': display}


def now():
    return datetime.now(timezone.utc)


def cipher():
    key = current_app.config.get('FRANCHISEE_PII_KEY')
    if not key:
        abort(503, description='Franchisee registration is temporarily unavailable. Contact AIT Admin.')
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except (ValueError, TypeError):
        abort(503, description='Franchisee registration is temporarily unavailable. Contact AIT Admin.')


def normal_email(raw):
    try:
        value = validate_email(raw or '', check_deliverability=False).normalized.lower()
        if len(value) > 254:
            raise ValueError()
        return value
    except (EmailNotValidError, ValueError):
        raise ValueError('Enter a valid email address.') from None


def linked_user(email):
    users = User.query.filter(func.lower(User.email) == email).all()
    if len(users) > 1:
        abort(409, description='The account link needs Admin review before registration can continue.')
    return users[0].id if users else None


def audit(row, action, actor=None):
    row.history = list(row.history or []) + [dict(action=action, actor_id=actor, at=now().isoformat())]


@franchisee_bp.before_request
def guard():
    if request.endpoint in {'franchisee_bp.opportunities', 'franchisee_bp.registration_start'}:
        if not current_user.is_authenticated:
            abort(401)
    elif request.endpoint not in {'franchisee_bp.register', 'franchisee_bp.submitted'}:
        if not current_user.is_authenticated:
            abort(401)
        if not is_admin(current_user):
            abort(403)
    if request.endpoint != 'franchisee_bp.submitted' and not db.session.execute(text(
            "SELECT to_regclass('ait_franchisee_registration') IS NOT NULL AND to_regclass('ait_franchisee_number_seq') IS NOT NULL")).scalar():
        abort(503, description='The Franchisee Register is awaiting its approved database migration.')


@franchisee_bp.after_request
def private_response(response):
    response.headers['Cache-Control'] = 'no-store, private'
    response.headers['Referrer-Policy'] = 'no-referrer'
    response.headers['X-Robots-Tag'] = 'noindex, nofollow'
    # Sensitive forms must not run third-party scripts from the global layout.
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'none'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
    return response


@franchisee_bp.route('/franchisees/opportunities', defaults={'registration_start': False})
@franchisee_bp.route('/franchisees/opportunities/register', endpoint='registration_start', defaults={'registration_start': True})
def opportunities(registration_start):
    registration = Registration.query.filter_by(user_id=current_user.id).first()
    return render_template('franchisees/opportunities.html', registration=registration,
                           registration_start=registration_start)


@franchisee_bp.route('/admin/franchisees', methods=['GET', 'POST'])
def register_index():
    if request.method == 'POST':
        cipher()  # Fail closed before issuing invitations without a persistent PII key.
        try:
            email = normal_email(request.form.get('email'))
        except ValueError as error:
            return render_template('franchisees/index.html', rows=Registration.query.order_by(Registration.id.desc()).all(), error=str(error)), 400
        if Registration.query.filter_by(email=email).first():
            abort(409, description='A registration for this email already exists. Review its status or resend its invitation.')
        row = Registration(email=email, user_id=linked_user(email), status='Invited', created_at=now(), created_by=current_user.id, history=[])
        db.session.add(row)
        return invite(row)
    return render_template('franchisees/index.html', rows=Registration.query.order_by(Registration.id.desc()).all())


def invite(row):
    token = secrets.token_urlsafe(32)
    row.invitation_digest = hashlib.sha256(token.encode()).hexdigest()
    row.invitation_expires_at = now() + timedelta(hours=48)
    row.invitation_sent_at = None
    audit(row, 'invited', current_user.id)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        abort(409, description='A registration already exists for this email or linked account.')
    # Build from a configured canonical host, never an untrusted Host header.
    origin = (current_app.config.get('FRANCHISEE_PUBLIC_ORIGIN') or '').rstrip('/')
    if not re.fullmatch(r'https://[A-Za-z0-9.-]+(?::[0-9]+)?', origin):
        abort(503, description='The invitation URL is not configured. Admin can resend after configuration.')
    link = origin + url_for('franchisee_bp.register', token=token)
    try:
        if current_app.config.get('MAIL_SUPPRESS_SEND'):
            raise RuntimeError('Mail unavailable')
        mail.send(Message(subject='AIT franchisee registration invitation', recipients=[row.email],
                          body='You are invited to register with AIT. Complete the form within 48 hours:\n\n' + link + '\n\nRegistration requires Admin approval and does not create a franchise agreement or change programme access or commercial terms.'))
    except Exception:
        # No recipient, token, message body or transport exception is logged.
        row = Registration.query.filter_by(id=row.id).populate_existing().with_for_update().one()
        audit(row, 'delivery_unconfirmed', current_user.id)
        db.session.commit()
        abort(503, description='Invitation delivery was not confirmed. Admin can explicitly resend; the previous link will then expire.')
    row = Registration.query.filter_by(id=row.id).populate_existing().with_for_update().one()
    if row.invitation_digest == hashlib.sha256(token.encode()).hexdigest():
        row.invitation_sent_at = now()
    audit(row, 'invitation_sent', current_user.id)
    db.session.commit()
    flash('Invitation sent. Registration is awaiting submission.', 'success')
    return redirect(url_for('franchisee_bp.register_index'))


@franchisee_bp.route('/admin/franchisees/<int:identifier>/resend', methods=['POST'])
def resend(identifier):
    cipher()
    row = Registration.query.filter_by(id=identifier).with_for_update().first_or_404()
    if row.status != 'Invited':
        abort(409, description='Only an unsubmitted invitation can be resent.')
    return invite(row)


def invitation(token):
    if not re.fullmatch(r'[A-Za-z0-9_-]{43}', token or ''):
        abort(400, description='This invitation is invalid, expired or already used.')
    digest = hashlib.sha256(token.encode()).hexdigest()
    row = Registration.query.filter_by(invitation_digest=digest).with_for_update().first()
    if not row or row.status != 'Invited' or not row.invitation_expires_at or row.invitation_expires_at <= now():
        abort(400, description='This invitation is invalid, expired or already used.')
    if current_user.is_authenticated and current_user.id != linked_user(row.email):
        abort(403, description='Open this invitation signed out, or sign in with the invited account.')
    return row


def identity_number(raw):
    value = (raw or '').strip()
    if not re.fullmatch(r'[0-9]{13}', value):
        raise ValueError('Enter a valid 13-digit SA ID number.')
    try:
        year = int(value[:2])
        year += 2000 if year <= now().year % 100 else 1900
        if date(year, int(value[2:4]), int(value[4:6])) > now().date():
            raise ValueError()
    except ValueError:
        raise ValueError('Enter a valid SA ID date of birth.') from None
    digits = [int(digit) for digit in value]
    checksum = sum(digits[::2]) + sum((2 * digit // 10 + 2 * digit % 10) for digit in digits[1::2])
    if checksum % 10:
        raise ValueError('Enter a valid SA ID number checksum.')
    return value


@franchisee_bp.route('/franchisees/register/<token>', methods=['GET', 'POST'])
def register(token):
    row = invitation(token)
    encryption = cipher()
    if request.method == 'POST':
        try:
            name = ' '.join((request.form.get('full_name') or '').split())
            phone = re.sub(r'[\s()-]', '', request.form.get('cellphone') or '')
            address = (request.form.get('address') or '').strip()
            occupation = (request.form.get('occupation') or '').strip()
            if len(name.split()) < 2 or len(name) > 160:
                raise ValueError('Enter your full name and surname (up to 160 characters).')
            if not re.fullmatch(r'(?:0[0-9]{9}|\+27[0-9]{9})', phone):
                raise ValueError('Enter a valid SA cellphone number, starting with 0 or +27.')
            if not 5 <= len(address) <= 1000 or len(occupation) > 160:
                raise ValueError('Enter a residential address (5–1000 characters) and an optional occupation (up to 160 characters).')
            if normal_email(request.form.get('email')) != row.email:
                raise ValueError('Use the email address that received this invitation.')
            identity = identity_number(request.form.get('sa_id'))
        except ValueError as error:
            return render_template('franchisees/register.html', error=str(error)), 400
        # HMAC, rather than a dictionary-attackable plain hash, identifies duplicates.
        user_id = linked_user(row.email)  # Resolve before mutations can trigger autoflush.
        key = current_app.config['FRANCHISEE_PII_KEY']
        row.identity_digest = hmac.new(base64.urlsafe_b64decode(key), b'franchisee-identity-v1:' + identity.encode(), hashlib.sha256).hexdigest()
        row.identity_ciphertext = encryption.encrypt(identity.encode()).decode()
        row.address_ciphertext = encryption.encrypt(address.encode()).decode()
        row.full_name, row.cellphone, row.occupation = name, phone, occupation or None
        row.user_id = user_id
        row.status, row.submitted_at, row.invitation_digest = 'Submitted', now(), None
        audit(row, 'submitted', current_user.id if current_user.is_authenticated else None)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            return render_template('franchisees/register.html', error='A registration already exists for these details. Contact AIT Admin.'), 409
        return redirect(url_for('franchisee_bp.submitted'))
    return render_template('franchisees/register.html')


@franchisee_bp.route('/franchisees/submitted')
def submitted():
    return render_template('franchisees/submitted.html')


@franchisee_bp.route('/admin/franchisees/<int:identifier>', methods=['GET', 'POST'])
def review(identifier):
    row = Registration.query.filter_by(id=identifier).with_for_update().first_or_404()
    identity = address = None
    if row.identity_ciphertext:
        try:
            encryption = cipher()
            identity = encryption.decrypt(row.identity_ciphertext.encode()).decode()
            address = encryption.decrypt(row.address_ciphertext.encode()).decode()
        except InvalidToken:
            abort(503, description='Private registration details cannot be read. Contact AIT Admin.')
    if request.method == 'POST':
        action = request.form.get('action')
        if row.status != 'Submitted' or action not in ('approve', 'reject'):
            abort(409, description='Only a submitted registration can be approved or rejected once.')
        if action == 'approve':
            number = db.session.execute(text("SELECT nextval('ait_franchisee_number_seq')")).scalar_one()
            row.franchisee_number = f'FR-{number:04d}'
        row.status = 'Approved' if action == 'approve' else 'Rejected'
        row.reviewed_at, row.reviewed_by = now(), current_user.id
        audit(row, row.status.lower(), current_user.id)
        db.session.commit()
        return redirect(url_for('franchisee_bp.review', identifier=row.id))
    return render_template('franchisees/review.html', row=row, identity=identity, address=address)
