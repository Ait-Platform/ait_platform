"""Bounded in-process index jobs, with durable PostgreSQL state and PDF cache.

Selection requests only claim/poll work. A crashed worker leaves a visible,
retryable job; it never leaves a request waiting on a PDF renderer/row lock.
"""
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
from copy import deepcopy
from datetime import datetime,timezone
from hashlib import sha256
import json,uuid
from flask import jsonify,current_app,abort,g
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
from . import lease_clause_index as index
POOL=ThreadPoolExecutor(max_workers=1,thread_name_prefix='lease-index')
SLOTS=BoundedSemaphore(2)


def history(draft):return draft['items'].get('_lease_history',{})


def building(job):
    try:return job.get('state')=='building' and (datetime.now(timezone.utc)-datetime.fromisoformat(job['started_at'])).total_seconds()<300
    except (ValueError,KeyError,TypeError):return False


def write_job(draft,job,actor,token=None):
    return db.session.execute(text("""UPDATE bil_lease_draft SET items=jsonb_set(items,
        '{_lease_history}',COALESCE(items->'_lease_history','{}'::jsonb)||jsonb_build_object('full_lease_index_job',CAST(:job AS jsonb)))
        WHERE id=:d AND property_id=:p AND manager_id=:u
        AND (CAST(:token AS text) IS NULL OR items->'_lease_history'->'full_lease_index_job'->>'token'=:token)"""),
        {'d':draft['id'],'p':draft['property_id'],'u':actor,'token':token,'job':json.dumps(job)})


def job_actor(identifier):
    from app.models.auth import User
    actor=db.session.query(User).filter_by(id=identifier).populate_existing().first()
    if actor is None or not actor.is_active:abort(403,description='The indexing user is no longer active.')
    return actor


def run(app,prop,draft,extractor,prepared,job,actor):
    try:
        with app.app_context():
            try:
                user=job_actor(actor)
                # Existing SC/map retrieval enforces current_user ownership. Revalidate
                # the recorded actor; never copy browser cookies or replay credentials.
                with app.test_request_context('/billing/leases/property/'+str(prop['id'])+'/addendum?draft_id='+str(draft['id'])):
                    g._login_user=user
                    from . import lease_snapshot as snapshot
                    snapshot.authorised_property(prop['id'])
                    index.load(prop,draft,extractor,with_pdf=True,prepared=prepared,actor_id=actor)
                job=dict(job,state='ready',finished_at=datetime.now(timezone.utc).isoformat())
            except Exception as error:
                db.session.rollback();current_app.logger.exception('Full Lease Clause Index background failure property=%s draft=%s',prop['id'],draft['id'])
                job=dict(job,state='failed',error=getattr(error,'description',None) or 'Clause Index generation failed ('+type(error).__name__+'). Please retry; the service log contains the backend exception.')
            write_job(draft,job,actor,job['token']);db.session.commit()
    except Exception:
        app.logger.exception('Clause Index job result could not be persisted draft=%s',draft['id'])
    finally:SLOTS.release()


def submit(*args):POOL.submit(run,*args)


def ready(saved,state):
    entries=[dict(id=e['id'],source=e['source'],source_label=e['source_label'],reference=e['reference'],title=e['title'],page=e['page'],source_version=saved['source_sha256'],search_text=e['original'],excerpt=' '.join(e['original'].split())[:350]) for e in saved['entries']]
    return jsonify(status='ready',results=entries,state=state,source_sha256=saved['source_sha256'],notices=saved['notices'])


def response(prop,draft,extractor,state,retry=False):
    prepared=index.inputs(prop,draft);version=prepared[1];saved=history(draft).get('full_lease_clause_index',{})
    if saved.get('input_version')==version and saved.get('schema')==index.SCHEMA and saved.get('pdf_parts'):
        return ready(saved,state)
    job=history(draft).get('full_lease_index_job',{})
    if job.get('input_version')==version:
        if building(job):return jsonify(status='building',state=state),202
        if job and not retry:return jsonify(error=job.get('error') or 'The indexing worker stopped or timed out. Use Retry Index to restart it.',retryable=True,state=state),503
    if not SLOTS.acquire(blocking=False):return jsonify(error='The index worker is busy. Please use Retry Index shortly.',retryable=True,state=state),429
    claimed=False
    try:
        row=db.session.execute(text('SELECT items,version FROM bil_lease_draft WHERE id=:d AND property_id=:p AND manager_id=:u FOR UPDATE'),{'d':draft['id'],'p':prop['id'],'u':current_user.id}).mappings().first()
        if not row:abort(404)
        if row['version']!=draft['version']:abort(409,description='The lease changed. Reopen Select Clause.')
        fresh_index=row['items'].get('_lease_history',{}).get('full_lease_clause_index',{})
        if fresh_index.get('input_version')==version and fresh_index.get('schema')==index.SCHEMA and fresh_index.get('pdf_parts'):return ready(fresh_index,state)
        other=row['items'].get('_lease_history',{}).get('full_lease_index_job',{})
        if other.get('input_version')==version and building(other):return jsonify(status='building',state=state),202
        job=dict(input_version=version,token=str(uuid.uuid4()),state='building',started_at=datetime.now(timezone.utc).isoformat())
        write_job(draft,job,current_user.id);db.session.commit()
        try:submit(current_app._get_current_object(),dict(prop),deepcopy(dict(draft)),extractor,prepared,job,int(current_user.id));claimed=True
        except Exception:
            job=dict(job,state='failed',error='The background worker could not start. Use Retry Index.');write_job(draft,job,current_user.id,job['token']);db.session.commit();raise
        return jsonify(status='building',state=state),202
    finally:
        if not claimed:SLOTS.release()
