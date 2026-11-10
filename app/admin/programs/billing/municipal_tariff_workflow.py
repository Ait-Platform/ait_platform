"""Annual verification and audited administrator source/variance review."""
from importlib import import_module
from flask import abort, request, redirect, url_for
from flask_login import current_user
from sqlalchemy.exc import SQLAlchemyError


def report(service, documents, verification, store, year, receipt):
    source=documents.metadata(store.get(documents.prefix(year)+'receipts/'+receipt+'.json'),year,receipt)
    data=documents.read(store,year,receipt)
    stored=service.records();evidence=service.verification_records()
    result=verification.compare(verification.extract(data,source),stored,evidence)
    return import_module('app.admin.programs.billing.municipal_tariff_policy').decorate(result,stored,evidence)


def page(render, year, receipt):
    service=import_module('app.admin.programs.billing.tariff_management');service.authorised()
    documents=import_module('app.admin.programs.billing.municipal_tariff_documents')
    verification=import_module('app.admin.programs.billing.municipal_tariff_verification')
    try:
        data=report(service,documents,verification,documents.backend(),year,receipt)
    except (ValueError,OSError,KeyError):
        abort(409,description='Retained source evidence could not be verified. Existing tariffs remain unchanged.')
    policy=import_module('app.admin.programs.billing.municipal_tariff_policy')
    error=None;error_status=409
    if request.method=='POST':
        try:
            service.lock_identity('annual-review',year)
            # A variance decision binds the last verified version, not hidden rates.
            locks=set()
            for row in data['rows']:
                source=row['existing'] or row['candidate']
                if source:locks.update(((source['utility_type'],'__code_allocation__'),(source['utility_type'],source['code'])))
            for utility,code in sorted(locks,key=lambda key:(key[0],key[1]!='__code_allocation__',key[1].casefold())):service.lock_identity(utility,code)
            data=report(service,documents,verification,documents.backend(),year,receipt)
            policy.decide(policy.backend(),int(current_user.id),year,receipt,request.form.get('action'),request.form.get('review_token'),data)
            service.db.session.rollback() # release locks; no tariff/database writes
            return redirect(url_for('admin_bp.billing_tariff_publication_verification',year=year.replace('/','-'),receipt=receipt))
        except (ValueError,OSError,SQLAlchemyError) as exc:
            service.db.session.rollback();error=str(exc) if isinstance(exc,ValueError) else 'Review storage or database lock failed. Existing tariffs and earlier decisions were preserved.'
            error_status=409 if isinstance(exc,ValueError) else 503
    data['source_token']=policy.issue_token(int(current_user.id),year,receipt,'approve_source',policy.digest(data['source']))
    for row in data['rows']:
        row['approve_token']=policy.issue_token(int(current_user.id),year,receipt,'approve',row['review']['key'])
        row['reject_token']=policy.issue_token(int(current_user.id),year,receipt,'reject',row['review']['key'])
    activation=import_module('app.admin.programs.billing.municipal_tariff_auto_activation')
    status='DISABLED' if not activation.enabled() else 'HOLD' if any(row['status']!='VERIFIED' for row in data['rows']) else 'Verified ? awaiting automatic check'
    return render('admin/programs/billing/municipal_tariff_verification.html',report=data,receipt=receipt,activation_status=status,error=error),error_status if error else 200


def schedule(service,documents,verification,store,year,receipt):
    """Join adopted source documents, retaining per-component source provenance."""
    primary=report(service,documents,verification,store,year,receipt)
    merged={};seen=set();adoption=None;observations=[]
    for record in documents.listing(store,year):
        if record['sha256'] in seen:continue
        seen.add(record['sha256'])
        data=documents.read(store,year,record['receipt_id'])
        extracted=verification.extract(data,record)
        observations.extend(extracted.get('observations',[]))
        if not extracted['adoption']:continue
        adoption=extracted['adoption']
        for code,values in extracted['extracted'].items():
            for value in values:
                value=dict(value,adoption=extracted['adoption'])
                existing=merged.setdefault(code,[])
                keys=('rate','unit','effective_date','category','block_start','block_end','supported')
                if not any(all(str(old[k])==str(value[k]) for k in keys) for old in existing):existing.append(value)
    if not adoption:return primary
    stored=service.records();evidence=service.verification_records()
    result=verification.compare(dict(year=year,source=primary['source'],adoption=adoption,extracted=merged,issues=[],observations=observations),stored,evidence)
    return import_module('app.admin.programs.billing.municipal_tariff_policy').decorate(result,stored,evidence)
