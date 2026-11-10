"""Read-only presentation counts; reuse existing extraction and verification services."""
from importlib import import_module
from types import SimpleNamespace
from flask import current_app


def build(year):
    result=dict(download_status='Not downloaded',extracted=0,passing=0,exceptions=None,updated=None)
    service,documents,verification,workflow,policy=[import_module('app.admin.programs.billing.'+name) for name in ('tariff_management','municipal_tariff_documents','municipal_tariff_verification','municipal_tariff_workflow','municipal_tariff_policy')]
    try:
        stored=service.records();evidence=service.verification_records()
        result['updated']=None if evidence is None else len({(row['utility_type'],row['code'].casefold()) for row in stored
            if year[:4]+'-07-01'<=str(row['effective_date'])<str(int(year[:4])+1)+'-07-01' and policy.verified(row,evidence)})
        store=documents.backend();records=documents.listing(store,year)
        if not records:return result
        result['download_status']='Downloaded'
        by_receipt={row['receipt_id']:row for row in records};pdfs={};extractions={}
        def read(store,on,receipt):
            row=by_receipt[receipt]
            if row['sha256'] not in pdfs:
                if len(pdfs)>=2:pdfs.pop(next(iter(pdfs))) # Bound memory for large municipal PDFs.
                pdfs[row['sha256']]=documents.read(store,on,receipt)
            content=pdfs[row['sha256']]
            if len(content)!=row['size_bytes']:raise ValueError('Retained document size conflicts.')
            return content
        def extract(content,metadata):
            key=(metadata['sha256'],metadata['source_url'],metadata['publication_status'])
            if key not in extractions:extractions[key]=verification.extract(content,metadata)
            return extractions[key]
        # Each retained PDF is counted once, even after repeated downloads.
        seen=set();count=0
        for record in records:
            if record['sha256'] in seen:continue
            seen.add(record['sha256'])
            data=extract(read(store,year,record['receipt_id']),record)
            count+=sum(len(values) for values in data['extracted'].values())
        result['extracted']=count
        document_view=SimpleNamespace(metadata=documents.metadata,prefix=documents.prefix,read=read,listing=lambda store,on:records)
        verification_view=SimpleNamespace(extract=extract,compare=verification.compare)
        report=workflow.schedule(service,document_view,verification_view,store,year,records[0]['receipt_id'])
        result['passing']=sum(row['status']=='VERIFIED' and row['review']['check']['result']=='PASS' for row in report['rows'])
        result['exceptions']=sum(row['status']!='VERIFIED' for row in report['rows'])
    except Exception as exc:
        current_app.logger.warning('Annual tariff summary unavailable (%s)',type(exc).__name__)
        # Do not turn unavailable/corrupt evidence into a successful zero count.
        result.update(download_status='Unavailable — review exceptions',extracted=None,passing=None,exceptions=None)
    return result
