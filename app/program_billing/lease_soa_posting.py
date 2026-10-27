"""Manual PM postings only. Existing signed-rand ledger; no recurring side effects."""
import hashlib
import json
import re
import uuid
from datetime import date
from decimal import Decimal
from flask import abort, current_app
from flask_login import current_user
from itsdangerous import URLSafeSerializer, BadSignature
from sqlalchemy import inspect, text
from app.extensions import db


def signer():
    return URLSafeSerializer(current_app.config['SECRET_KEY'], salt='pm-ledger-post-v1')


def token(property_id, tenant_id, kind):
    return signer().dumps([current_user.id, property_id, tenant_id, kind, uuid.uuid4().hex])


def ledger_model():
    from app.models.billing import BilTenantLedger
    return BilTenantLedger


def post(property_id, tenant_id, form):
    """Validate, authorise and atomically insert once, including retries across workers."""
    kind = form.get('kind', '')
    if kind not in ('charge', 'payment'):
        raise ValueError('Choose Post Charge or Record Payment.')
    try:
        scope = signer().loads(form.get('posting_key', ''))
        if (not isinstance(scope, list) or len(scope) != 5 or
                scope[:4] != [current_user.id, property_id, tenant_id, kind] or
                not re.fullmatch(r'[a-f0-9]{32}', scope[4])):
            raise BadSignature('scope')
    except (BadSignature, TypeError):
        raise ValueError('This posting form is invalid. Open a new posting form.')
    raw = form.get('amount', '').strip()
    if not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,2})?', raw):
        raise ValueError('Enter a positive amount with at most two decimal places, up to R99,999,999.99.')
    amount = Decimal(raw).quantize(Decimal('0.01'))
    if amount <= 0:
        raise ValueError('The amount must be greater than zero.')
    try:
        raw_date = form.get('txn_date', '')
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', raw_date):
            raise ValueError()
        txn_date = date.fromisoformat(raw_date)
    except ValueError:
        raise ValueError('Enter a valid transaction date.')
    description = form.get('description', '').strip()
    reference = form.get('reference', '').strip()
    if not description or len(description) > 255 or '\x00' in description:
        raise ValueError('Description is required and must contain at most 255 characters.')
    if len(reference) > 100 or '\x00' in reference or (kind == 'payment' and not reference):
        raise ValueError('A payment reference is required; references may contain at most 100 characters.')

    connection = db.session.connection()
    if connection.dialect.name != 'postgresql':
        abort(503, description='PM posting requires PostgreSQL.')
    # The post-lock lookup must see a retry committed by another worker.
    if connection.get_isolation_level() != 'READ COMMITTED':
        abort(503, description='PM posting requires PostgreSQL READ COMMITTED isolation. No transaction was saved.')
    columns = {c['name']: c for c in inspect(connection).get_columns('bil_tenant_ledger')}
    required = {'id', 'tenant_id', 'txn_date', 'month', 'description', 'kind', 'amount', 'ref'}
    if (not required <= columns.keys() or
            getattr(columns.get('amount', {}).get('type'), 'scale', None) != 2 or
            (getattr(columns.get('amount', {}).get('type'), 'precision', 0) or 0) < 10 or
            (getattr(columns.get('ref', {}).get('type'), 'length', None) or 255) < 208 or
            not (columns.get('id', {}).get('default') or columns.get('id', {}).get('identity')) or
            any(not c.get('nullable') and not c.get('default') and not c.get('identity')
                for n, c in columns.items() if n not in required)):
        abort(503, description='The PM ledger schema does not support safe manual posting. No transaction was saved.')

    # Lock ownership and draft records used by the existing association resolver.
    prop = db.session.execute(text("""SELECT id FROM bil_property
        WHERE id=:p AND manager_id=:m AND NOT is_archived FOR SHARE"""),
        dict(p=property_id, m=current_user.id)).first()
    tenant = db.session.execute(text("""SELECT id FROM bil_tenant
        WHERE id=:t AND manager_id=:m FOR SHARE"""),
        dict(t=tenant_id, m=current_user.id)).first()
    if not prop or not tenant:
        abort(404)
    db.session.execute(text("""SELECT id FROM bil_lease_draft
        WHERE property_id=:p AND manager_id=:m FOR SHARE"""),
        dict(p=property_id, m=current_user.id)).all()
    from .lease_soa import tenants_for
    if not any(row['id'] == tenant_id for row in tenants_for(property_id)):
        abort(404)

    payload = [current_user.id, property_id, tenant_id, kind, raw_date,
               str(amount), description, reference]
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()
    prefix = 'PMPOST:' + scope[4] + ':'
    stored_ref = prefix + digest + ' | ' + reference
    lock = int.from_bytes(hashlib.sha256(prefix.encode()).digest()[:8], 'big', signed=True)
    db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'), dict(key=lock))
    existing = db.session.execute(text("""SELECT ref FROM bil_tenant_ledger
        WHERE tenant_id=:t AND ref LIKE :prefix"""),
        dict(t=tenant_id, prefix=prefix + '%')).scalars().all()
    if existing:
        if existing != [stored_ref]:
            abort(409, description='This posting key was already used for different details. No new transaction was saved.')
        db.session.commit()
        return False
    entry = ledger_model()(tenant_id=tenant_id, txn_date=txn_date, month=raw_date[:7],
        description=description, kind=kind,
        amount=amount if kind == 'charge' else -amount, ref=stored_ref)
    db.session.add(entry)
    db.session.commit()
    return True
