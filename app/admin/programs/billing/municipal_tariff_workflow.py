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
    return render('admin/programs/billing/municipal_tariff_verification.html',report=data,receipt=receipt)
