"""Read-only annual verification UI over retained official source documents."""
from importlib import import_module
from flask import abort, request


def report(service, documents, verification, store, year, receipt):
    source=documents.metadata(store.get(documents.prefix(year)+'receipts/'+receipt+'.json'),year,receipt)
    data=documents.read(store,year,receipt)
    return verification.compare(verification.extract(data,source),service.records(),service.verification_records())


def page(render, year, receipt):
    service=import_module('app.admin.programs.billing.tariff_management');service.authorised()
    documents=import_module('app.admin.programs.billing.municipal_tariff_documents')
    verification=import_module('app.admin.programs.billing.municipal_tariff_verification')
    try:
        data=report(service,documents,verification,documents.backend(),year,receipt)
    except (ValueError,OSError,KeyError):
        abort(409,description='Retained source evidence could not be verified. Existing tariffs remain unchanged.')
    activation=import_module('app.admin.programs.billing.municipal_tariff_auto_activation')
    status='DISABLED' if not activation.enabled() else 'HOLD' if any(row['status']!='VERIFIED' for row in data['rows']) else 'Verified ? awaiting automatic check'
    return render('admin/programs/billing/municipal_tariff_verification.html',report=data,receipt=receipt,activation_status=status)


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
    return verification.compare(dict(year=year,source=primary['source'],adoption=adoption,extracted=merged,issues=[],observations=observations),service.records(),service.verification_records())
