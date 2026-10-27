"""Append-only corrections to the existing PM ledger, never in-place financial edits."""
import hashlib
import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from flask import abort
from flask_login import current_user
from itsdangerous import BadSignature
from sqlalchemy import inspect, text
from app.extensions import db
from . import lease_soa_posting as posting

AUDIT_COLUMNS={'correction_of','correction_role','correction_group','correction_actor',
               'correction_recorded_at','correction_reason'}


def require_schema():
    posting.require_posting_schema()
    catalog=inspect(db.session.connection())
    if (not AUDIT_COLUMNS<={c['name'] for c in catalog.get_columns('bil_tenant_ledger')} or
            not {'uq_pm_ledger_correction_role','uq_pm_ledger_correction_group'}<={
                i['name'] for i in catalog.get_indexes('bil_tenant_ledger') if i['unique']} or
            'fk_pm_ledger_correction_tenant' not in {i['name'] for i in catalog.get_foreign_keys('bil_tenant_ledger')} or
            'ck_pm_ledger_correction_audit' not in {i['name'] for i in catalog.get_check_constraints('bil_tenant_ledger')}):
        abort(503,description='Controlled corrections require migration pm_soa_002. No transaction was changed.')


def target(tenant_id, row_id, lock=False):
    require_schema()
    row=db.session.execute(text("""SELECT id,tenant_id,txn_date,description,kind,amount,ref,
        correction_role FROM bil_tenant_ledger WHERE id=:id AND tenant_id=:t"""+
        (' FOR UPDATE' if lock else '')),dict(id=row_id,t=tenant_id)).mappings().first()
    if row is None:abort(404)
    try:
        amount=Decimal(str(row['amount']))
        if not amount.is_finite() or abs(amount)>Decimal('99999999.99'):raise InvalidOperation()
    except (InvalidOperation,ValueError,TypeError):
        abort(503,description='The source amount cannot be reversed safely. No correction was saved.')
    return dict(row)


def fingerprint(row):
    return hashlib.sha256(json.dumps(
        [row['id'],row['tenant_id'],str(row['txn_date']),row['description'],
         row['kind'],str(row['amount']),row['ref']],ensure_ascii=False).encode()).hexdigest()


def form_token(property_id, tenant_id, row):
    return posting.token(property_id,tenant_id,'correction:'+str(row['id'])+':'+fingerprint(row))


def correct(property_id, tenant_id, row_id, form):
    require_schema()
    posting.authorise(property_id,tenant_id)
    try:
        scope=posting.signer().loads(form.get('posting_key',''))
        expected=[current_user.id,property_id,tenant_id]
        if (not isinstance(scope,list) or len(scope)!=5 or scope[:3]!=expected or
                not isinstance(scope[3],str) or not scope[3].startswith('correction:'+str(row_id)+':') or
                not re.fullmatch('[a-f0-9]{32}',scope[4])):
            raise BadSignature('scope')
    except (BadSignature,TypeError):
        raise ValueError('This correction form is invalid. Open Edit again.')
    direction=form.get('direction','')
    if direction not in ('debit','credit','zero'):
        raise ValueError('Choose a debit or credit type.')
    raw=form.get('amount','').strip()
    if not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,2})?',raw):
        raise ValueError('Enter an amount with at most two decimal places, up to R99,999,999.99.')
    amount=Decimal(raw).quantize(Decimal('0.01'))
    if (direction=='zero' and amount!=0) or (direction!='zero' and amount<=0):
        raise ValueError('Enter a positive debit/credit amount, or zero for an opening marker correction.')
    try:
        raw_date=form.get('txn_date','')
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',raw_date):raise ValueError()
        effective_date=date.fromisoformat(raw_date)
    except ValueError:
        raise ValueError('Enter a valid correction date.')
    reason=form.get('reason','').strip()
    if not reason or len(reason)>2000 or '\x00' in reason:
        raise ValueError('A correction reason is required (at most 2000 characters).')

    # Serialise retries, then corrections of this source transaction.
    lock=int.from_bytes(hashlib.sha256(('PMCORRECT:'+scope[4]).encode()).digest()[:8],'big',signed=True)
    db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    original=target(tenant_id,row_id,lock=True)
    opening=original['kind'] in ('opening_balance','opening_adjustment') or (
        (original['description'] or '').strip().lower()=='opening balance')
    description='Opening Balance correction' if opening else form.get('description','').strip()
    if (not description or len(description)>255 or '\x00' in description or
            description.lower()=='opening balance'):
        raise ValueError('Enter a description of at most 255 characters; Opening Balance is reserved.')
    if direction=='zero' and not opening:
        raise ValueError('Only an opening-balance correction may have a zero amount.')
    digest=hashlib.sha256(json.dumps([scope[3],raw_date,description,direction,str(amount),reason],
        ensure_ascii=False).encode()).hexdigest()
    existing=db.session.execute(text("""SELECT correction_role,posting_hash FROM bil_tenant_ledger
        WHERE correction_group=:key ORDER BY correction_role"""),dict(key=scope[4])).all()
    if existing:
        if existing!=[('replacement',digest),('reversal',digest)]:
            abort(409,description='This correction form was already used for different details. No new correction was saved.')
        db.session.commit()
        return False
    if scope[3]!='correction:'+str(row_id)+':'+fingerprint(original):
        abort(409,description='The source transaction changed after Edit was opened. Review the current transaction before retrying.')
    if original['correction_role']=='reversal':
        abort(409,description='A correction reversal is an audit entry. Edit its replacement transaction instead.')
    if db.session.execute(text('SELECT EXISTS(SELECT 1 FROM bil_tenant_ledger WHERE correction_of=:id)'),
            dict(id=row_id)).scalar():
        abort(409,description='This transaction has already been corrected. Edit its replacement transaction.')
    try:
        original_amount=Decimal(str(original['amount']))
        if not original_amount.is_finite() or abs(original_amount)>Decimal('99999999.99'):
            raise InvalidOperation()
    except (InvalidOperation,ValueError,TypeError):
        abort(503,description='The source amount cannot be reversed safely. No correction was saved.')

    # Both rows and all audit metadata commit together or are rolled back together.
    for role,signed,kind,desc,key in (
        ('reversal',-original_amount,'correction_reversal',
         'Reversal of '+str(original['ref'] or 'transaction '+str(row_id))[:220],
         hashlib.sha256((scope[4]+':reversal').encode()).hexdigest()[:32]),
        ('replacement',-amount if direction=='credit' else amount,
         'opening_adjustment' if opening else 'correction_replacement',description,scope[4])):
        entry=posting.ledger_model()(tenant_id=tenant_id,txn_date=effective_date,
            month=raw_date[:7],description=desc,kind=kind,amount=signed,
            ref=posting.next_reference('credit' if signed<0 else 'debit'))
        db.session.add(entry)
        db.session.flush()
        db.session.execute(text("""UPDATE bil_tenant_ledger
            SET posting_key=:key,posting_hash=:digest,correction_of=:original,
                correction_role=:role,correction_group=:group,correction_actor=:actor,
                correction_recorded_at=CURRENT_TIMESTAMP,correction_reason=:reason
            WHERE id=:id"""),dict(key=key,digest=digest,original=row_id,role=role,
                group=scope[4],actor=current_user.id,reason=reason,id=entry.id))
    db.session.commit()
    return True
