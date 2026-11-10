"""Daily annual municipal checks; native worker/CLI, opt-in and no email retries."""
from contextlib import contextmanager
from datetime import date,datetime
from hashlib import sha256
from importlib import import_module
import json
import os
import threading
import time
from zoneinfo import ZoneInfo
import click
from flask import current_app
from flask.cli import with_appcontext
from sqlalchemy import text

ROOT='municipal-tariff-monitor/'


def services():
    names=('tariff_management','municipal_tariff_sources','municipal_tariff_documents','municipal_tariff_verification','municipal_tariff_workflow','municipal_tariff_auto_activation')
    return tuple(import_module('app.admin.programs.billing.'+name) for name in names)


def local_today():return datetime.now(ZoneInfo('Africa/Johannesburg')).date()


def history(store):
    results={}
    for key in store.keys(ROOT+'checks/'):
        row=json.loads(store.get(key))
        previous=results.get(row['year'])
        if previous is None or row['checked_on']>previous['checked_on']:
            row['last_successful_check']=max([value for value in (row['checked_on'] if row.get('successful_retrieval') else None,previous.get('last_successful_check') if previous else None) if value],default=None)
            results[row['year']]=row
        elif row.get('successful_retrieval'):
            previous['last_successful_check']=max(previous.get('last_successful_check') or '',row['checked_on'])
    return results


def targets(today, previous):
    start=today.year-(today.month<7)
    years={str(start)+'/'+str(start+1)[-2:]}
    if today.month in (5,6):years.add(str(today.year)+'/'+str(today.year+1)[-2:])
    # Late publication checks continue across later financial-year boundaries.
    years.update(year for year,row in previous.items() if not row['verified'])
    return sorted(years)


@contextmanager
def singleton(service):
    """Dedicated session lock survives tariff transaction commits; always released."""
    with service.db.engine.connect() as connection:
        acquired=connection.execute(text("SELECT pg_try_advisory_lock(hashtextextended('pm-municipal-tariff-monitor',0))")).scalar()
        try:yield acquired
        finally:
            if acquired:connection.execute(text("SELECT pg_advisory_unlock(hashtextextended('pm-municipal-tariff-monitor',0))"))
            connection.commit()


def notify(store, year, reason):
    """Public operational metadata only, stable incident key, no repeated alert."""
    identity=sha256((year+'|'+reason).encode()).hexdigest()
    key=ROOT+'alerts/'+identity+'.json'
    if key in store.keys(ROOT+'alerts/'):return False
    payload=dict(year=year,reason=reason,first_seen=local_today().isoformat(),notification='Administrator Annual Tariff Register')
    store.put(key,json.dumps(payload,sort_keys=True).encode(),'application/json')
    current_app.logger.warning('Municipal tariff administrator alert year=%s reason=%s',year,reason)
    return True


def check_year(store,year,today):
    service,source,documents,verification,workflow,activation=services()
    issues=[];saved=[];deadline=time.monotonic()+75
    found,errors=source.discovery(year,deadline=deadline)
    retrieval_details=list(errors)
    if errors:issues.append('Official publication retrieval failed or is incomplete.')
    if not found:issues.append('No official annual tariff publication was retrieved.')
    policy=import_module('app.admin.programs.billing.municipal_tariff_policy')
    try:review_store=policy.backend()
    except (ValueError,OSError):review_store=None
    for candidate in found[:source.MAX_DOWNLOADS]:
        try:source_trusted=review_store is not None and policy.trusted(review_store,candidate)
        except (ValueError,OSError):
            issues.append('Source approval audit could not be verified; administrator investigation is required.')
            continue
        if not source_trusted:
            issues.append('First-time administrator approval of the official download source is required.')
            continue
        try:
            data,details=source.retrieve(candidate,deadline=deadline)
            saved.append(documents.save(store,data,details))
        except source.RetrievalError as exc:
            retrieval_details.append(candidate['source_url']+': '+str(exc))
            current_app.logger.warning('Municipal source retrieval failed year=%s source=%s reason=%s',year,candidate['source_url'],str(exc))
            issues.append('A municipal publication could not be downloaded or validated.')
        except Exception:
            current_app.logger.exception('Municipal evidence retention failed')
            issues.append('Official document storage or integrity verification failed.')
    if len(found)>source.MAX_DOWNLOADS:issues.append('Additional municipal publications require review; automatic batch limit reached.')
    report=None;result=dict(status='DISABLED' if not activation.enabled() else 'HOLD',created=0)
    if saved:
        try:
            report=workflow.schedule(service,documents,verification,store,year,saved[0]['receipt_id'])
            if not report['adoption']:issues.append('Publication adoption is not confirmed for the specific annual tables.')
            unresolved=[row['code'] for row in report['rows'] if row['status']!='VERIFIED']
            if unresolved:issues.append('Unverified rates, units or unsupported/conflicting mappings: '+', '.join(unresolved))
            # Do not activate a partial discovery batch after a retrieval failure.
            if not issues and all(row['status']=='VERIFIED' for row in report['rows']):
                result=activation.automatic(service,documents,store,year,saved[0]['receipt_id'])
        except Exception:
            current_app.logger.exception('Automatic municipal verification/update failed')
            issues.append('Automatic verification or tariff update failed; no partial schedule was committed.')
    verified=bool(report and not issues and all(row['status']=='VERIFIED' for row in report['rows']))
    start=date(source.year_start(year),7,1)
    if not verified and date(start.year,6,1)<=today<start:issues.append('The 1 June target has passed; continue checking for the adopted annual publication.')
    if not verified and today>=start:issues.append('Municipal publication is late or remains unverified after 1 July.')
    issues=sorted(set(issues))
    for reason in issues:notify(store,year,reason)
    row=dict(year=year,checked_on=today.isoformat(),effective_date=start.isoformat(),verified=verified,
             activation=result,issues=issues,source_urls=[r['source_url'] for r in saved],
             successful_retrieval=bool(saved),retrieval_details=retrieval_details,verified_components=[r['code'] for r in report['rows'] if r['status']=='VERIFIED'] if report else [])
    store.put(ROOT+'checks/'+year.replace('/','-')+'/'+today.isoformat()+'.json',json.dumps(row,sort_keys=True).encode(),'application/json')
    return row


def run(today=None):
    service,source,documents,verification,workflow,activation=services()
    today=today or local_today();output=[]
    with singleton(service) as acquired:
        if not acquired:return dict(status='BUSY',checks=[])
        store=documents.backend();previous=history(store)
        for year in targets(today,previous):
            old=previous.get(year)
            if old and old['checked_on']==today.isoformat():continue
            if old and old['verified'] and (not activation.enabled() or old['activation']['status'] in ('ACTIVE','SCHEDULED')):continue
            output.append(check_year(store,year,today))
    return dict(status='CHECKED',checks=output)


def summary():
    """Read-only administrator notices; no municipal HTTP, scheduler or updates."""
    try:
        documents=import_module('app.admin.programs.billing.municipal_tariff_documents')
        return sorted(history(documents.backend()).values(),key=lambda row:row['checked_on'],reverse=True)
    except Exception:
        return [dict(year='Monitoring',checked_on='Unavailable',issues=['Monitoring history unavailable; check durable document storage and server logs.'],activation=dict(status='UNKNOWN'),verified_components=[],source_urls=[])]


@click.command('pm-check-municipal-tariffs')
@with_appcontext
def command():
    """Run one daily municipal check using the existing application CLI."""
    try:click.echo(json.dumps(run(),sort_keys=True))
    except Exception as exc:
        current_app.logger.exception('Municipal annual monitoring failed')
        raise click.ClickException('Municipal monitoring failed; check storage, database and server logs.') from exc


def install(app):
    # Both deployment switches are OFF unless explicitly configured/authorised.
    for name in ('PM_TARIFF_MONITOR_ENABLED','PM_TARIFF_AUTO_ACTIVATION_ENABLED'):
        app.config.setdefault(name,os.getenv(name,'').lower()=='true')
    app.config.setdefault('PM_TARIFF_AUTOMATION_ACTOR_ID',os.getenv('PM_TARIFF_AUTOMATION_ACTOR_ID'))
    app.cli.add_command(command)
    if not app.config['PM_TARIFF_MONITOR_ENABLED'] or app.extensions.get('pm_tariff_monitor'):return
    stop=threading.Event();app.extensions['pm_tariff_monitor']=stop
    def worker():
        while not stop.wait(30 if not getattr(worker,'started',False) else 3600):
            worker.started=True
            with app.app_context():
                try:run()
                except Exception:app.logger.exception('Municipal tariff monitoring unavailable; administrator attention required')
                finally:
                    try:services()[0].db.session.remove()
                    except Exception:app.logger.exception('Municipal monitor session cleanup failed')
    thread=threading.Thread(target=worker,name='pm-municipal-tariffs',daemon=True)
    thread.start()
