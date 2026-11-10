"""Source trust and variance clearance; no tariff records or calculations live here."""
from datetime import date,datetime,timezone
from decimal import Decimal,InvalidOperation
from hashlib import sha256
from importlib import import_module
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
from flask import current_app
from itsdangerous import URLSafeSerializer,URLSafeTimedSerializer,BadSignature

PREFIX='municipal-tariff-review/'


def digest(value):return sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()


def backend():
    documents=import_module('app.admin.programs.billing.municipal_tariff_documents')
    root=current_app.config.get('PM_TARIFF_REVIEW_ROOT') or os.getenv('PM_TARIFF_REVIEW_ROOT')
    document_root=current_app.config.get('PM_TARIFF_DOCUMENT_ROOT') or os.getenv('PM_TARIFF_DOCUMENT_ROOT')
    if not root and document_root:
        root=Path(document_root)/'private-review'
    if not root and os.getenv('RENDER'):raise ValueError('Private tariff review storage must be configured on persistent disk. Public R2 evidence storage cannot hold administrator decisions.')
    return documents.Disk(root or Path(current_app.instance_path)/'pm_tariff_review')


def signer():return URLSafeSerializer(current_app.secret_key,salt='pm-tariff-review-ledger-v1')


def form_signer():return URLSafeTimedSerializer(current_app.secret_key,salt='pm-tariff-review-form-v1')


def events(store,scope):
    result=[]
    for key in store.keys(PREFIX+scope+'/'):
        try:
            row=signer().loads(store.get(key).decode())
            if row.get('scope')!=scope or key!=PREFIX+scope+'/'+row['id']+'.json':raise ValueError('Audit scope mismatch.')
            result.append(row)
        except (BadSignature,ValueError,UnicodeError):raise ValueError('Tariff review audit integrity failed.') from None
    return sorted(result,key=lambda row:(row['recorded_at'],row['id']))


def trusted(store,source):
    sources=import_module('app.admin.programs.billing.municipal_tariff_sources')
    pdf=urlsplit(sources.official_url(source['source_url'],pdf=True))
    index=urlsplit(sources.official_url(source['index_url']))
    return bool(index.path=='/pages/government/documents' and any(row['action']=='approve_source' and pdf.hostname in row['hosts'] and index.hostname in row['hosts'] for row in events(store,'sources')))


def verified(row,evidence):
    audit=evidence.get(row['id'],{})
    if not all(audit.get(key) for key in ('municipality','municipal_category','source_reference','verified_by','verified_at')):return False
    municipality=''.join(c for c in str(audit['municipality']).casefold() if c.isalnum())
    if municipality not in ('ethekwini','ethekwinimunicipality','ethekwinimetropolitanmunicipality'):return False
    try:
        amount=Decimal(str(row['rate']))
        if not amount.is_finite() or amount<0:return False
    except (ValueError,InvalidOperation):return False
    reference=str(audit['source_reference'])
    if 'CURRENT_RATE_SUBSTITUTE' in reference or 'CURRENT-RATE SUBSTITUTE' in str(row.get('description','')):return False
    if reference.startswith('AIT_ANNUAL_TARIFF_ALIGNMENT_V1:'):
        try:
            if json.loads(reference.split(':',1)[1]).get('estimated'):return False
        except ValueError:return False
    return str(audit.get('effective_date'))==str(row['effective_date'])


def variance(old,new):
    new=Decimal(str(new))
    if not new.is_finite() or new<0:raise ValueError('Invalid proposed rate.')
    if old is None:return dict(percent=None,result='NEW',clearance=True)
    old=Decimal(str(old))
    if not old.is_finite() or old<0:raise ValueError('Invalid previous verified rate.')
    if old==0:
        return dict(percent=Decimal(0) if new==0 else None,result='PASS' if new==0 else 'ZERO BASE',clearance=new!=0)
    change=(new-old)/old*100
    return dict(percent=change,result='DECREASE' if change<0 else 'ABOVE 10%' if change>10 else 'PASS',clearance=change<0 or change>10)


def context(row,stored,evidence):
    register=import_module('app.admin.programs.billing.annual_tariff_register')
    candidate=row['candidate'];old=None;problems=[]
    if candidate:
        versions=[record for record in stored if register.component(record,evidence.get(record['id'],{}))==row['code'] and str(record['effective_date'])<=candidate['effective_date'] and verified(record,evidence)]
        chosen,problems=register.resolve(versions,date.fromisoformat(candidate['effective_date']))
        if len(chosen)>1:problems.append('Multiple verified tariff identities require investigation.')
        if len(chosen)==1 and not problems:old=chosen[0]
    check=variance(old['rate'] if old else None,candidate['rate']) if candidate else dict(percent=None,result='MISSING',clearance=True)
    snapshot=dict(code=row['code'],candidate=candidate,previous=old,verification=evidence.get(old['id'],{}) if old else None)
    return dict(previous=old,check=check,key=digest(snapshot),problems=problems)


def decorate(report,stored,evidence,store=None):
    evidence=evidence or {};error=None
    try:store=store or backend();source_trusted=trusted(store,report['source'])
    except (ValueError,OSError) as exc:source_trusted=False;error=str(exc);store=None
    report['source_trusted']=source_trusted;report['source_approval_needed']=not source_trusted
    report['review_storage_error']=error
    report['source_approval_evidence']=events(store,'sources')[-1]['id'] if store and source_trusted else None
    for row in report['rows']:
        base_ready=row['status']=='VERIFIED'
        candidate=row['candidate'];old=row['existing']
        row['recorded']=bool(candidate and old and str(old['effective_date'])==candidate['effective_date'] and Decimal(str(old['rate']))==Decimal(candidate['rate']) and verified(old,evidence))
        if candidate and old and str(old['effective_date'])==candidate['effective_date'] and Decimal(str(old['rate']))!=Decimal(candidate['rate']):
            row['reasons'].append('A different rate already exists at this effective date. Use the existing tariff editor; history cannot be overwritten.')
            base_ready=False
        info=context(row,stored,evidence);row.update(review=info,clearance=None,can_clear=False)
        if store:
            history=events(store,'rates/'+info['key'])
            if history:row['clearance']=history[-1]['action'];row['clearance_evidence']=history[-1]['id']
        if not source_trusted:row['reasons'].append(error or 'Official municipal download source awaits first-time administrator approval.')
        row['reasons'].extend(info['problems'])
        row['can_clear']=bool(not row['recorded'] and base_ready and source_trusted and not info['problems'] and info['check']['clearance'])
        if info['check']['clearance']:
            if row['clearance']!='approve':row['reasons'].append('Administrator variance clearance '+('rejected.' if row['clearance']=='reject' else 'required: '+info['check']['result']+'.'))
        if row['reasons']:row['status']='HOLD'
    return report


def issue_token(actor,year,receipt,action,key):
    return form_signer().dumps(dict(actor=actor,year=year,receipt=receipt,action=action,key=key,nonce=uuid4().hex))


def decide(store,actor,year,receipt,action,signed,report):
    if action not in ('approve_source','approve','reject'):raise ValueError('Unsupported review action.')
    try:bound=form_signer().loads(signed,max_age=900)
    except (BadSignature,TypeError):raise ValueError('Review expired. Reload the tariff register.') from None
    if any(bound.get(key)!=value for key,value in dict(actor=actor,year=year,receipt=receipt,action=action).items()):raise ValueError('Review does not belong to this administrator or source.')
    identifier=digest(dict(token=signed,action=action))
    scope='sources' if action=='approve_source' else 'rates/'+bound['key']
    key=PREFIX+scope+'/'+identifier+'.json'
    if key in store.keys(PREFIX+scope+'/'):return False
    if action=='approve_source':
        if bound['key']!=digest(report['source']):raise ValueError('Municipal source evidence changed. Reload the register.')
        sources=import_module('app.admin.programs.billing.municipal_tariff_sources')
        sources.official_url(report['source']['source_url'],pdf=True);sources.official_url(report['source']['index_url'])
        details=dict(source=report['source'],hosts=sorted(sources.HOSTS))
    else:
        row=next((row for row in report['rows'] if row['review']['key']==bound['key']),None)
        if not row or row.get('recorded') or (action=='approve' and not row['can_clear']):raise ValueError('Rate evidence, applicable category or prior verified version changed; unresolved adoption/unit/mapping cannot be cleared by variance approval.')
        details=dict(code=row['code'],candidate=row['candidate'],previous=row['review']['previous'],percentage=str(row['review']['check']['percent']))
    record=dict(id=identifier,scope=scope,action=action,recorded_by=actor,recorded_at=datetime.now(timezone.utc).isoformat(),year=year,receipt=receipt,**details)
    # PostgreSQL returns Decimal/date/datetime values in tariff/audit rows.
    payload=json.loads(json.dumps(record,default=str))
    store.put(key,signer().dumps(payload).encode(),'application/json')
    current_app.logger.info('Municipal tariff review actor=%s action=%s evidence=%s',actor,action,identifier)
    return True
