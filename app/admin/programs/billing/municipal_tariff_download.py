"""Admin-only Stage 3A controller. No tariff extraction, approval or scheduling."""
from datetime import datetime
from importlib import import_module
from io import BytesIO
import time
from zoneinfo import ZoneInfo
from flask import abort,current_app,request,send_file
from flask_login import current_user


def services():
    return (import_module('app.admin.programs.billing.tariff_management'),
            import_module('app.admin.programs.billing.municipal_tariff_sources'),
            import_module('app.admin.programs.billing.municipal_tariff_documents'))


def page(render):
    management,source,documents=services();management.authorised()
    today=datetime.now(ZoneInfo('Africa/Johannesburg')).date()
    current=today.year-(today.month<7)
    years=[str(year)+'/'+str(year+1)[-2:] for year in range(2024,current+2)]
    year=request.values.get('financial_year',str(current)+'/'+str(current+1)[-2:])
    if year not in years:abort(400,description='Select an available current, historical or upcoming financial year.')
    issues=[];saved=[];records=[];discovered=[]
    try:store=documents.backend()
    except (documents.StorageError,OSError) as exc:
        store=None;issues.append(str(exc))
    if store is not None:
        if request.method=='POST':
            deadline=time.monotonic()+75
            discovered,discovery_issues=source.discovery(year,deadline=deadline)
            issues.extend(discovery_issues)
            for candidate in discovered[:source.MAX_DOWNLOADS]:
                try:
                    content,details=source.retrieve(candidate,deadline=deadline)
                    record=documents.save(store,content,details);saved.append(record)
                    current_app.logger.info('Municipal tariff publication retained actor=%s year=%s sha256=%s status=%s',current_user.id,year,record['sha256'],record['publication_status'])
                except source.RetrievalError as exc:issues.append(candidate['source_url']+': '+str(exc))
                except Exception as exc:
                    current_app.logger.warning('Municipal evidence storage failed (%s)',type(exc).__name__)
                    issues.append('Document storage/read-back failed. No complete receipt was accepted for '+candidate['source_url']+'. Existing tariffs are unchanged.')
            if len(discovered)>source.MAX_DOWNLOADS:
                issues.append('Download limited to '+str(source.MAX_DOWNLOADS)+' publications per request. Additional source links are shown below.')
        try:records=documents.listing(store,year)
        except Exception:
            issues.append('Stored municipal evidence could not be listed or verified. Existing files and tariffs were not replaced.')
    return render('admin/programs/billing/municipal_tariff_download.html',years=years,financial_year=year,
                  documents=records,saved=saved,issues=issues,discovered=discovered,storage_ready=store is not None)


def download(year,receipt):
    management,source,documents=services();management.authorised()
    try:content=documents.read(documents.backend(),year,receipt)
    except (documents.StorageError,OSError,KeyError):abort(404,description='Verified stored municipal publication is unavailable.')
    except Exception:abort(503,description='Municipal document storage is unavailable.')
    response=send_file(BytesIO(content),mimetype='application/pdf',as_attachment=True,
                       download_name='eThekwini-'+year.replace('/','-')+'-'+receipt[:8]+'.pdf')
    response.headers['X-Content-Type-Options']='nosniff'
    return response
