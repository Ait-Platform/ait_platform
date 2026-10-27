"""Manual PM postings. Existing signed-rand ledger; no recurring side effects."""
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
from sqlalchemy.exc import IntegrityError, DBAPIError
from app.extensions import db

DEBIT_DESCRIPTIONS = ('Rent Due', 'Water Charges', 'Electricity Charges',
    'Sewerage Charges', 'Refuse Removal', 'Other Utility Charges',
    'Recoverable Repairs', 'Late Payment Charges', 'Other Charge')
CREDIT_DESCRIPTIONS = ('Payment Received', 'Charge Reversal', 'Credit Adjustment',
    'Rental Discount', 'Overpayment Received', 'Other Credit')
OPENING_PREDICATE = "kind='opening_balance' OR lower(btrim(description))='opening balance'"


def signer():
    return URLSafeSerializer(current_app.config['SECRET_KEY'], salt='pm-ledger-post-v1')


def token(property_id, tenant_id, kind):
    return signer().dumps([current_user.id, property_id, tenant_id, kind, uuid.uuid4().hex])


def ledger_model():
    from app.models.billing import BilTenantLedger
    return BilTenantLedger


def has_opening(tenant_id):
    return db.session.execute(text('SELECT EXISTS(SELECT 1 FROM bil_tenant_ledger '
        'WHERE tenant_id=:t AND ('+OPENING_PREDICATE+'))'), dict(t=tenant_id)).scalar()


REFERENCE_SEQUENCES = {'D':'pm_ledger_debit_ref_seq','C':'pm_ledger_credit_ref_seq'}


def next_reference(direction):
    prefix_letter = 'C' if direction=='credit' else 'D'
    sequence = REFERENCE_SEQUENCES[prefix_letter]
    try:
        number = db.session.execute(text('SELECT nextval(CAST(:sequence AS regclass))'),
            dict(sequence=sequence)).scalar_one()
    except DBAPIError as exc:
        if getattr(exc.orig,'pgcode',None)=='2200H':
            abort(409,description='The '+prefix_letter+' reference sequence has reached 9999. No transaction was saved.')
        raise
    if number<1 or number>9999:abort(503,description='The PM reference sequence is outside its permitted range.')
    return prefix_letter+str(number).zfill(4)


def post(property_id, tenant_id, form):
    """Validate, authorise and atomically insert once, including cross-worker retries."""
    kind = form.get('kind', '')
    if kind not in ('opening_balance', 'charge', 'payment'):
        raise ValueError('Choose Opening Balance, Post Charge or Record Payment.')
    try:
        scope = signer().loads(form.get('posting_key', ''))
        if (not isinstance(scope, list) or len(scope) != 5 or
                scope[:4] != [current_user.id, property_id, tenant_id, kind] or
                not re.fullmatch(r'[a-f0-9]{32}', scope[4])):
            raise BadSignature('scope')
    except (BadSignature, TypeError):
        raise ValueError('This posting form is invalid. Open a new posting form.')
    direction = form.get('direction', '')
    if kind != 'opening_balance':
        direction = 'debit' if kind=='charge' else 'credit'
    elif direction not in ('debit', 'credit', 'zero'):
        raise ValueError('Choose Debit, Credit or Zero for the opening balance.')
    raw = form.get('amount', '').strip()
    if not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,2})?', raw):
        raise ValueError('Enter an amount with at most two decimal places, up to R99,999,999.99.')
    amount = Decimal(raw).quantize(Decimal('0.01'))
    if direction=='zero':
        if amount != 0:raise ValueError('A zero opening marker must have an amount of zero.')
    elif amount <= 0:
        raise ValueError('The amount must be greater than zero.')
    try:
        raw_date = form.get('txn_date', '')
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', raw_date):raise ValueError()
        txn_date = date.fromisoformat(raw_date)
    except ValueError:
        raise ValueError('Enter a valid transaction date.')
    description = 'Opening Balance' if kind=='opening_balance' else form.get('description', '')
    allowed = DEBIT_DESCRIPTIONS if direction=='debit' else CREDIT_DESCRIPTIONS
    if kind!='opening_balance' and description not in allowed:
        raise ValueError('Choose a description from the list.')
    notes = form.get('notes', '').strip()
    if len(notes)>2000 or '\x00' in notes:
        raise ValueError('Explanatory notes must contain at most 2000 characters.')

    require_posting_schema()
    authorise(property_id, tenant_id)

    payload = [current_user.id,property_id,tenant_id,kind,raw_date,
               str(amount),description,direction,notes]
    digest = hashlib.sha256(json.dumps(payload,ensure_ascii=False).encode()).hexdigest()
    prefix = 'PMPOST:'+scope[4]+':'
    lock = int.from_bytes(hashlib.sha256(prefix.encode()).digest()[:8],'big',signed=True)
    db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    existing = db.session.execute(text("""SELECT posting_hash FROM bil_tenant_ledger
        WHERE posting_key=:key"""),dict(key=scope[4])).scalars().all()
    # Preserve duplicate protection for references stored by the preceding release.
    legacy = db.session.execute(text("""SELECT ref FROM bil_tenant_ledger
        WHERE tenant_id=:t AND ref LIKE :prefix"""),
        dict(t=tenant_id,prefix=prefix+'%')).scalars().all()
    if existing or legacy:
        legacy_payload = [current_user.id,property_id,tenant_id,kind,raw_date,
                          str(amount),description,form.get('reference','').strip()]
        old_hash = hashlib.sha256(json.dumps(legacy_payload,ensure_ascii=False).encode()).hexdigest()
        if existing!=[digest] and legacy!=[prefix+old_hash+' | '+form.get('reference','').strip()]:
            abort(409,description='This posting key was already used for different details. No new transaction was saved.')
        db.session.commit()
        return False

    if kind=='opening_balance':
        # Tenant-wide, independent of property and form nonce. The unique index is
        # the final backstop, including writers that do not use advisory locks.
        db.session.execute(text("SELECT pg_advisory_xact_lock(1836020592,:tenant)"),dict(tenant=tenant_id))
        if has_opening(tenant_id):
            abort(409,description='An Opening Balance is already recorded for this tenant.')
    reference = next_reference(direction)
    entry = ledger_model()(tenant_id=tenant_id,txn_date=txn_date,month=raw_date[:7],
        description=description,kind=kind,
        amount=-amount if direction=='credit' else amount,ref=reference)
    try:
        db.session.add(entry)
        db.session.flush()
        # Metadata belongs only to this newly inserted row; the established ORM
        # stays compatible for legacy readers while the migration is being applied.
        db.session.execute(text("""UPDATE bil_tenant_ledger
            SET posting_key=:key,posting_hash=:digest,posting_notes=:notes WHERE id=:id"""),
            dict(key=scope[4],digest=digest,notes=notes or None,id=entry.id))
        db.session.commit()
    except IntegrityError as exc:
        if getattr(getattr(exc.orig,'diag',None),'constraint_name',None)=='uq_pm_ledger_opening':
            abort(409,description='An Opening Balance is already recorded for this tenant.')
        raise
    return True


def require_posting_schema():
    connection = db.session.connection()
    if connection.dialect.name != 'postgresql' or connection.get_isolation_level()!='READ COMMITTED':
        abort(503, description='PM posting requires PostgreSQL READ COMMITTED isolation.')
    catalog = inspect(connection)
    columns = {c['name']:c for c in catalog.get_columns('bil_tenant_ledger')}
    required = {'id','tenant_id','txn_date','month','description','kind','amount','ref',
                'posting_key','posting_hash','posting_notes'}
    indexes = {i['name'] for i in catalog.get_indexes('bil_tenant_ledger') if i['unique']}
    if (not required<=columns.keys() or
            not {'uq_pm_ledger_posting_key','uq_pm_ledger_opening','uq_pm_ledger_dc_ref'}<=indexes or
            getattr(columns.get('amount',{}).get('type'),'scale',None)!=2 or
            (getattr(columns.get('amount',{}).get('type'),'precision',0) or 0)<10 or
            (getattr(columns.get('ref',{}).get('type'),'length',None) or 255)<5 or
            not (columns.get('id',{}).get('default') or columns.get('id',{}).get('identity'))):
        abort(503, description='The PM ledger schema does not support safe posting. Apply migration pm_soa_001 before posting.')


def authorise(property_id, tenant_id):
    prop = db.session.execute(text("""SELECT id FROM bil_property
        WHERE id=:p AND manager_id=:m AND NOT is_archived FOR SHARE"""),
        dict(p=property_id,m=current_user.id)).first()
    tenant = db.session.execute(text("""SELECT id FROM bil_tenant
        WHERE id=:t AND manager_id=:m FOR SHARE"""),
        dict(t=tenant_id,m=current_user.id)).first()
    if not prop or not tenant:abort(404)
    db.session.execute(text("""SELECT id FROM bil_lease_draft
        WHERE property_id=:p AND manager_id=:m FOR SHARE"""),
        dict(p=property_id,m=current_user.id)).all()
    from .lease_soa import tenants_for
    if not any(row['id']==tenant_id for row in tenants_for(property_id)):abort(404)
