"""Atomic annual activation in existing BilTariff/audit tables; disabled by default."""
from datetime import date
from decimal import Decimal
from hashlib import sha256
from importlib import import_module
import json
from flask import current_app
from sqlalchemy import text

KEYS=('utility_type','code','description','rate','block_start','block_end','reduction_factor','unit','effective_date')


def enabled():
    return current_app.config.get('PM_TARIFF_AUTO_ACTIVATION_ENABLED',False) is True


def audit_actor(service):
    """Existing audit schema requires an accountable existing user, never a fake ID."""
    identifier=current_app.config.get('PM_TARIFF_AUTOMATION_ACTOR_ID')
    if not identifier:raise ValueError('Automatic activation audit actor is not configured.')
    from app.models.auth import User
    actor=service.db.session.get(User,int(identifier))
    if (actor is None or not (getattr(actor,'is_admin',False) is True or str(getattr(actor,'role','')).casefold()=='admin')
            or not bool(getattr(actor,'is_active',True))):
        raise ValueError('Configured automation audit actor is not an active administrator.')
    return int(actor.id)


def activate(service, documents, verification, store, year, receipt, actor_id):
    """Re-read exact evidence and current DB under locks; commit the whole batch once.

    No report JSON, hidden form rates or stale approval snapshot is accepted.
    Caller must enforce the deployment enable gate and configured audit actor.
    """
    from .municipal_tariff_workflow import schedule as report
    if not enabled():return dict(status='DISABLED',created=0,skipped=0)
    try:
        service.lock_identity('annual-schedule',year)
        initial=report(service,documents,verification,store,year,receipt)
        if not initial['adoption'] or any(row['status']!='VERIFIED' for row in initial['rows']):
            raise ValueError('Annual schedule remains on HOLD: unresolved adoption, rates or calculation mappings.')
        if set(verification.REQUIRED)-{r['code'] for r in initial['rows']}:raise ValueError('Required annual components are missing.')
        locks=set()
        for row in initial['rows']:
            old=row['existing'];candidate=row['candidate']
            utility=old['utility_type'] if old else candidate['utility_type']
            code=old['code'] if old else row['code']
            locks.update(((utility,'__code_allocation__'),(utility,code)))
        for utility,code in sorted(locks):service.lock_identity(utility,code)
        prepared=report(service,documents,verification,store,year,receipt)
        if not prepared['adoption'] or any(row['status']!='VERIFIED' for row in prepared['rows']):
            raise ValueError('Annual evidence or stored versions changed while acquiring locks.')
        if {(r['existing']['utility_type'] if r['existing'] else r['candidate']['utility_type'],r['existing']['code'] if r['existing'] else r['code']) for r in prepared['rows']} - locks:
            raise ValueError('Tariff identities changed while acquiring locks; retry with fresh evidence.')
        # Never choose one of conflicting adopted municipal publications silently.
        for record in documents.listing(store,year):
            if record['receipt_id']==receipt:continue
            other=report(service,documents,verification,store,year,record['receipt_id'])
            if not other['adoption']:continue
            other_rows={row['code']:row for row in other['rows']}
            for row in prepared['rows']:
                alternative=other_rows.get(row['code'],{}).get('candidate')
                if alternative and alternative['supported'] and any(str(alternative[key])!=str(row['candidate'][key]) for key in ('rate','unit','effective_date','category','block_start','block_end')):
                    raise ValueError('Conflicting adopted municipal publications require investigation.')
        evidence=service.verification_records()
        if evidence is None:raise ValueError('Tariff verification infrastructure is unavailable.')
        stored=service.records();created=0;skipped=0
        snapshot=sha256(json.dumps(prepared,sort_keys=True,default=str).encode()).hexdigest()
        for row in prepared['rows']:
            candidate=row['candidate'];old=row['existing']
            if not candidate or not candidate['supported']:raise ValueError('Unsupported annual component.')
            new=dict(old) if old else dict(utility_type=candidate['utility_type'],code=row['code'],description=row['code']+' — '+candidate['category'],block_start=candidate['block_start'],block_end=candidate['block_end'],reduction_factor=Decimal(candidate['reduction_factor']),unit=candidate['unit'])
            if not str(new.get('unit') or '').strip():new['unit']=candidate['unit']
            if row['code'].startswith(('W0','S0')):
                if Decimal(str(new.get('block_start')))!=candidate['block_start']:raise ValueError('Stored band boundary conflicts.')
                if row['code'].startswith('W') and (Decimal(str(new.get('reduction_factor')))!=Decimal(candidate['reduction_factor']) or new.get('block_end')!=candidate['block_end']):raise ValueError('Stored water band metadata conflicts.')
                if Decimal(str(new.get('reduction_factor')))<=0:raise ValueError('Invalid stored calculation factor.')
            new.update(rate=Decimal(candidate['rate']),effective_date=candidate['effective_date'])
            reference=(candidate['source_url']+'#page='+str(candidate['page'])+' | AIT automatic annual '+year+'; SHA256 '+candidate['sha256']+'; parser '+prepared['parser']+'; adoption '+candidate.get('adoption',prepared['adoption'])['date']+'; unit '+candidate['unit']+'; snapshot '+snapshot)
            same=[v for v in stored if v['utility_type']==new['utility_type'] and v['code'].casefold()==new['code'].casefold() and str(v['effective_date'])==new['effective_date']]
            if same:
                if len(same)!=1 or any(str(same[0].get(key))!=str(new.get(key)) for key in ('unit','block_start','block_end')) or Decimal(str(same[0]['rate']))!=new['rate']:
                    raise ValueError('Conflicting tariff version already exists at this effective date.')
                previous=evidence.get(same[0]['id'],{}).get('source_reference') or ''
                if candidate['sha256'] not in previous or 'AIT automatic annual '+year not in previous:
                    raise ValueError('Existing annual version has different/unverified provenance; no record was overwritten.')
                skipped+=1;continue
            identifier=service.db.session.execute(text('INSERT INTO bil_tariff ('+','.join(KEYS)+') VALUES ('+','.join(':'+key for key in KEYS)+') RETURNING id'),{key:new[key] for key in KEYS}).scalar_one()
            service.db.session.execute(text('INSERT INTO bil_tariff_verification (tariff_id,municipality,municipal_category,source_reference,effective_date,verified_by) VALUES (:id,:municipality,:category,:source,:effective,:actor)'),dict(id=identifier,municipality='eThekwini Municipality',category=candidate['category'],source=reference,effective=new['effective_date'],actor=actor_id))
            created+=1
        service.db.session.commit()
        return dict(status='SCHEDULED' if date.fromisoformat(year[:4]+'-07-01')>service.today() else 'ACTIVE',created=created,skipped=skipped,snapshot=snapshot)
    except Exception:
        service.db.session.rollback()
        raise


def automatic(service,documents,store,year,receipt):
    if not enabled():return dict(status='DISABLED',created=0,skipped=0)
    verification=import_module('app.admin.programs.billing.municipal_tariff_verification')
    return activate(service,documents,verification,store,year,receipt,audit_actor(service))
