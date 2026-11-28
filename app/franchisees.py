"""Global account-linked Franchise registration and administrative accountability."""
import hashlib
import re
import secrets
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet, InvalidToken
from email_validator import validate_email, EmailNotValidError
from flask import Blueprint, abort, current_app, flash, g, redirect, render_template, request, url_for
from flask_login import current_user
from flask_mail import Message
from flask_wtf.csrf import CSRFError
from sqlalchemy import func, or_, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.extensions import db, mail
from app.utils.roles import is_admin
from app.models.auth import User
from app.models.franchisee import FranchiseeRegistration as CanonicalRegistration
from app.franchise_crypto import (ProfileProtectionError, shared_key, encrypt_private,
    decrypt_private)
from werkzeug.local import LocalProxy
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.orm import registry
from threading import Lock

_legacy_model = None
_legacy_model_lock = Lock()


def legacy_model():
    # Separate metadata keeps the old table out of application create_all/autogeneration.
    global _legacy_model
    with _legacy_model_lock:
        if _legacy_model is None:
            metadata = MetaData()
            Table('user',metadata,Column('id',Integer,primary_key=True))
            table = CanonicalRegistration.__table__.to_metadata(metadata,name='ait_franchisee_registration')
            class LegacyRegistration:
                is_active = CanonicalRegistration.is_active
                is_suspended = CanonicalRegistration.is_suspended
                query = db.session.query_property(query_cls=type(CanonicalRegistration.query))
            registry(metadata=metadata).map_imperatively(LegacyRegistration,table)
            _legacy_model = LegacyRegistration
    return _legacy_model


Registration = LocalProxy(lambda: getattr(g,'franchise_registration_model',CanonicalRegistration))

franchisee_bp = Blueprint('franchisee_bp', __name__)


class RegistrationValidationError(ValueError):
    def __init__(self, errors):
        super().__init__('Please correct the highlighted fields.')
        self.errors = errors


@franchisee_bp.context_processor
def local_dates():
    def display(value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return value.astimezone(ZoneInfo('Africa/Johannesburg')).strftime('%Y-%m-%d %H:%M SAST')
    return {'franchisee_date': display}


def now():
    return datetime.now(timezone.utc)


TERMS_VERSION = '2026-11-27'


def cipher():
    return Fernet(shared_key())


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


def audit(row, action, actor=None, **details):
    row.history = list(row.history or []) + [dict(action=action, actor_id=actor, at=now().isoformat(), **details)]


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
    if request.endpoint != 'franchisee_bp.submitted':
        canonical,legacy,sequence = db.session.execute(text(
            "SELECT to_regclass('franchise_registration'),to_regclass('ait_franchisee_registration'),to_regclass('ait_franchisee_number_seq')")).one()
        if not sequence or not (canonical or legacy) or (canonical and legacy):
            abort(503,description='Franchise registration schema is unavailable or ambiguous. Contact AIT Admin; no migration is run automatically.')
        g.franchise_registration_model = CanonicalRegistration if canonical else legacy_model()
        definition = db.session.execute(text("""SELECT pg_get_constraintdef(oid)
          FROM pg_constraint WHERE conrelid=to_regclass(:table) AND conname='ck_franchisee_submitted'
          AND convalidated"""),{'table':'franchise_registration' if canonical else 'ait_franchisee_registration'}).scalar()
        g.franchise_simplified_ready = bool(definition and 'identity_ciphertext' not in definition and 'identity_digest' not in definition)



@franchisee_bp.errorhandler(ProfileProtectionError)
def protection_error(error):
    db.session.rollback()
    if request.endpoint == 'franchisee_bp.opportunities':
        registration = current_registration()
        return dashboard_response(registration,'edit' if request.form.get('action')=='save_profile' else 'register',str(error),503)
    return 'Private profile protection is unavailable. No changes were saved. Contact AIT Admin.',503


@franchisee_bp.after_request
def private_response(response):
    response.headers['Cache-Control'] = 'no-store, private'
    # Flask-WTF's strict HTTPS CSRF check needs the origin; private paths stay omitted.
    response.headers['Referrer-Policy'] = 'strict-origin'
    response.headers['X-Robots-Tag'] = 'noindex, nofollow'
    # Sensitive forms must not run third-party scripts from the global layout.
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'none'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
    return response


def current_registration(lock=False):
    # Stable account identity takes precedence over editable contact details.
    query = Registration.query
    if lock:
        query = query.populate_existing().with_for_update()
    row = query.filter_by(user_id=current_user.id).first()
    if row:
        return row
    try:
        email = normal_email(current_user.email)
    except ValueError:
        return None
    if linked_user(email) != current_user.id:
        abort(409, description='The account link needs Admin review.')
    matches = query.filter(func.lower(func.trim(Registration.email)) == email).all()
    if len(matches) > 1 or (matches and matches[0].user_id not in (None,current_user.id)):
        abort(409, description='The account link needs Admin review.')
    # Preserve older unlinked registrations; bind only on an explicit successful Save.
    return matches[0] if matches else None


@franchisee_bp.errorhandler(CSRFError)
def registration_csrf_error(error):
    if request.endpoint == 'franchisee_bp.opportunities' and current_user.is_authenticated:
        if not hasattr(g,'franchise_registration_model'):
            guard()
        registration = current_registration()
        return dashboard_response(registration, 'register',
                                  'Your form security check failed. Reload Register and submit again.', 400)
    return error.get_response()


@franchisee_bp.route('/franchisees/opportunities/register', endpoint='registration_start')
def registration_start():
    registration = current_registration()
    return redirect(url_for('franchisee_bp.opportunities', **({} if registration and registration.franchisee_number else {'area':'register'})))


@franchisee_bp.route('/franchisees/opportunities', methods=['GET', 'POST'])
def opportunities():
    area = request.args.get('area', '')
    if area not in {'', 'register', 'edit', 'close', 'clients', 'earnings', 'protrade', 'programmes'}:
        abort(400)
    registration = current_registration()
    if area == 'close':
        return dashboard_response(registration, '')
    if area == 'edit' and (not registration or registration.status == 'Invited'):
        abort(409, description='Register before editing a saved profile.')
    if request.method == 'GET' and not area and not (registration and registration.franchisee_number):
        area = 'register'
    error, status, errors = None, 200, {}
    if request.method == 'POST':
        area = 'register'
        # Serialize self-registration for this account, including double submissions.
        User.query.filter_by(id=current_user.id).with_for_update().one()
        if request.form.get('action') == 'save_profile':
            registration = current_registration(lock=True)
            if not registration or registration.status == 'Invited':
                abort(409, description='No saved Franchise profile is available to update.')
            try:
                save_registration(registration, editing=True)
            except ValueError as exc:
                db.session.rollback()
                return dashboard_response(current_registration(), 'edit', str(exc), 400, getattr(exc,'errors',{}))
            except IntegrityError:
                db.session.rollback()
                return dashboard_response(current_registration(), 'edit', 'These contact or identity details are already registered.', 409)
            except SQLAlchemyError as exc:
                db.session.rollback()
                return dashboard_response(current_registration(),'edit',database_failure(exc,editing=True),503)
            flash('Franchise profile updated. Your original registration number and date are unchanged.', 'success')
            return redirect(url_for('franchisee_bp.opportunities', area='register'))
        try:
            email = normal_email(current_user.email)
        except ValueError:
            return dashboard_response(registration, area, 'Your AIT account needs a valid email address.', 400)
        if linked_user(email) != current_user.id:
            abort(409, description='The account link needs Admin review.')
        matches = Registration.query.filter(or_(Registration.user_id == current_user.id,
                                                func.lower(func.trim(Registration.email)) == email)).with_for_update().all()
        if len(matches) > 1 or (matches and matches[0].user_id not in (None, current_user.id)):
            abort(409, description='The account link needs Admin review.')
        registration = matches[0] if matches else Registration(
            email=email, user_id=current_user.id, status='Invited', created_at=now(),
            created_by=current_user.id, history=[])
        if registration.status == 'Invited' and not g.franchise_simplified_ready:
            return dashboard_response(current_registration(),area,
                'Simplified registration needs the approved franchise_registration_003 database migration. No registration was saved. Existing records are preserved.',503)
        if registration.status not in {'Invited', 'Submitted'}:
            return dashboard_response(registration, area, 'This registration has already been completed or reviewed.', 409)
        try:
            if request.form.get('accept_terms') != 'yes':
                raise RegistrationValidationError({'accept_terms':'Accept the AIT terms and Franchise participation conditions to register.'})
            if registration.status == 'Submitted' and request.form.get('action') == 'activate':
                registration.email = email
                registration.user_id = current_user.id
                activate_registration(registration, current_user.id, automatic=True)
                db.session.commit()
            elif registration.status == 'Invited':
                save_registration(registration, activate=True)
            else:
                return dashboard_response(registration, area, 'Use Complete Registration to activate your saved details.', 409)
        except ValueError as exc:
            db.session.rollback()
            error, status, errors = str(exc), 400, getattr(exc, 'errors', {})
        except IntegrityError:
            db.session.rollback()
            error, status = 'A registration already exists for these details.', 409
        except SQLAlchemyError as exc:
            db.session.rollback()
            return dashboard_response(current_registration(),area,database_failure(exc),503)
        else:
            flash('Registration complete. Your franchisee account is active.', 'success')
            return redirect(url_for('franchisee_bp.opportunities', area='register'))
        registration = current_registration()
    return dashboard_response(registration, area, error, status, errors)


def database_failure(error, editing=False):
    code = getattr(getattr(error,'orig',None),'pgcode',None)
    if code == '42501':
        return 'The database role lacks a required registration permission. No save was confirmed. Contact AIT Admin.'
    return ('Profile changes' if editing else 'Registration') + ' could not be confirmed. Reload the saved register before trying again.'


def programme_data():
    # Educational seat licences are a separate legacy workflow, not a registration prerequisite.
    from app.models.auth import AuthSubject, UserEnrollment
    enrolments = {r.subject_id:r for r in UserEnrollment.query.filter_by(user_id=current_user.id).order_by(UserEnrollment.id).all()}
    subjects = AuthSubject.query.filter(AuthSubject.is_active == 1,
        AuthSubject.is_hidden_on_bridge == False).order_by(AuthSubject.name,AuthSubject.id).all()
    return [dict(subject=row,enrolment=enrolments.get(row.id))
        for row in subjects if row.slug not in {'admin','admin_general','staff','home_premium'}]


def dashboard_response(registration, area, error=None, status=200, errors=None):
    identity, address = None, None
    editing = area == 'edit'
    values = {}
    if editing and registration and registration.address_ciphertext:
        try:
            address = decrypt_private(registration.address_ciphertext)
        except InvalidToken:
            error, status = 'Your registration is saved, but its private details could not be read. Contact AIT Admin.', 503
        else:
            values = dict(full_name=registration.full_name,cellphone=registration.cellphone,
                occupation=registration.occupation or '',address=address)
    shops, earnings, programmes = [], [], []
    if registration and registration.is_active:
        if area == 'programmes':
            programmes = programme_data()
        if area in {'clients','protrade','earnings'}:
            from app.models.mechanic import MechShop, ProTradeDocument
            # Existing businesses belong to the AIT account, never to a number or display name.
            if area in {'clients','protrade'}:
                shops = MechShop.query.filter_by(user_id=current_user.id).order_by(MechShop.business_name,MechShop.id).all()
            else:
                earnings = db.session.query(
                    ProTradeDocument.document_key, ProTradeDocument.created_at, MechShop.business_name,
                    ProTradeDocument.snapshot['commercial_terms']['operator_retains_cents'].as_integer().label('earnings_cents')
                ).join(MechShop,MechShop.id==ProTradeDocument.shop_id).filter(
                    ProTradeDocument.user_id==current_user.id,MechShop.user_id==current_user.id,
                    ProTradeDocument.kind=='boq_section').order_by(ProTradeDocument.created_at.desc(),ProTradeDocument.id.desc()).all()
    return render_template('franchisees/opportunities.html',registration=registration,area=area,
        shops=shops,earnings=earnings,programmes=programmes,error=error,errors=errors or {},
        identity=identity,address=address,editing=editing,registration_values=values,
        profile_unavailable=editing and registration is not None and not values,
        registration_unavailable=registration is None and status==503,
        simplified_ready=getattr(g,'franchise_simplified_ready',False)),status


@franchisee_bp.route('/admin/franchisees', methods=['GET', 'POST'])
def register_index():
    if request.method == 'POST':
        cipher()  # Require shared AIT protection before issuing an optional legacy invitation.
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


def activate_registration(row, actor, automatic=False):
    if not row.franchisee_number:
        with db.session.no_autoflush:
            number = db.session.execute(text("SELECT nextval('ait_franchisee_number_seq')")).scalar_one()
        row.franchisee_number = f'FR-{number:04d}'
    row.status, row.reviewed_at = 'Approved', now()
    row.reviewed_by = None if automatic else actor
    audit(row, 'automatically_activated' if automatic else 'approved', actor,
          **({'terms_version':TERMS_VERSION, 'terms_accepted':True} if automatic else {}))


def save_registration(row, activate=False, editing=False):
    if not current_user.is_authenticated:
        abort(401,description='Sign in to your existing AIT account to register.')
    if editing:
        try:
            previous_address = decrypt_private(row.address_ciphertext)
        except InvalidToken:
            abort(503,description='Saved address cannot be read; no changes were saved. Contact AIT Admin.')
    name = ' '.join((request.form.get('full_name') or '').split())
    phone = re.sub(r'[\s()-]','',request.form.get('cellphone') or '')
    address = (request.form.get('address') or '').strip()
    occupation = (request.form.get('occupation') or '').strip()
    errors = {}
    if not name or len(name)>160:
        errors['full_name']='Enter your full name (up to 160 characters).'
    if not re.fullmatch(r'(?:0[0-9]{9}|\+27[0-9]{9})',phone):
        errors['cellphone']='Enter a valid SA cellphone number, starting with 0 or +27.'
    if not 5<=len(address)<=1000:
        errors['address']='Enter a residential address (5-1000 characters).'
    if len(occupation)>160:
        errors['occupation']='Use up to 160 characters for your occupation.'
    try:
        email=normal_email(current_user.email)
    except ValueError:
        errors['account']='Your AIT account needs a valid email address. Update your AIT account before saving.'
    if errors:
        raise RegistrationValidationError(errors)
    if not editing or address!=previous_address:
        row.address_ciphertext=encrypt_private(address)
    # SA ID fields are historical only. Never read, validate, digest, clear or
    # overwrite them during registration/editing, even if forged fields are posted.
    row.full_name,row.cellphone,row.occupation,row.email=name,phone,occupation or None,email
    row.user_id=current_user.id
    if editing:
        audit(row,'profile_updated',current_user.id)
    else:
        row.status,row.submitted_at,row.invitation_digest='Submitted',now(),None
        audit(row,'submitted',current_user.id)
        db.session.add(row)
        if activate:
            activate_registration(row,current_user.id,automatic=True)
    db.session.commit()


@franchisee_bp.route('/franchisees/register/<token>', methods=['GET', 'POST'])
def register(token):
    # Preserve old invitation URLs/records, but all new profile entry uses the
    # authenticated, account-email workflow rather than a second public form.
    if not current_user.is_authenticated:
        abort(401,description='Sign in to your existing AIT account, then open Franchise Register.')
    invitation(token)
    return redirect(url_for('franchisee_bp.opportunities',area='register'))


@franchisee_bp.route('/franchisees/submitted')
def submitted():
    return render_template('franchisees/submitted.html')


@franchisee_bp.route('/admin/franchisees/<int:identifier>', methods=['GET', 'POST'])
def review(identifier):
    row = Registration.query.filter_by(id=identifier).with_for_update().first_or_404()
    identity = address = None
    if row.identity_ciphertext or row.address_ciphertext:
        try:
            identity = decrypt_private(row.identity_ciphertext) if row.identity_ciphertext else None
            address = decrypt_private(row.address_ciphertext) if row.address_ciphertext else None
        except InvalidToken:
            abort(503, description='Private registration details cannot be read. Contact AIT Admin.')
    if request.method == 'POST':
        action = request.form.get('action')
        if row.status == 'Approved' and action in {'suspend', 'reinstate'}:
            if (action == 'suspend') == row.is_suspended:
                abort(409, description='This registration is already in the requested state.')
            audit(row, 'suspended' if action == 'suspend' else 'reinstated', current_user.id)
        elif row.status == 'Submitted' and action in {'approve', 'reject'}:
            if action == 'approve':
                activate_registration(row, current_user.id)
            else:
                row.status, row.reviewed_at, row.reviewed_by = 'Rejected', now(), current_user.id
                audit(row, 'rejected', current_user.id)
        else:
            abort(409, description='This action is not available for the registration status.')
        db.session.commit()
        return redirect(url_for('franchisee_bp.review', identifier=row.id))
    return render_template('franchisees/review.html', row=row, identity=identity, address=address)
