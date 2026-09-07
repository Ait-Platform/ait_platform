"""Real overlapping PostgreSQL sessions; committed synthetic schema discarded after each test."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from flask import Flask
from werkzeug.exceptions import HTTPException
from bootstrap import db, core, uip
from conftest import safe_url, baseline, migrate, migrate_phase3, data as seed_data
from phase3_helpers import provider, order, act, completed, key
from app.uip.services import work_orders, operations


@pytest.fixture
def concurrent_db():
    url=safe_url(os.environ.get("UIP_TEST_DATABASE_URL"))
    schema="uip_test_"+uuid.uuid4().hex
    control=sa.create_engine(url)
    with control.begin() as conn:conn.exec_driver_sql('CREATE SCHEMA "'+schema+'"')
    engine=sa.create_engine(url,connect_args={"options":"-csearch_path="+schema+" -cstatement_timeout=10000"})
    original=db.session
    db.session=scoped_session(sessionmaker(bind=engine,expire_on_commit=False))
    app=Flask("uip_concurrency")
    try:
        with engine.begin() as conn:baseline(conn);migrate(conn);migrate_phase3(conn)
        with app.app_context():
            data=seed_data.__wrapped__(app)
            p=provider(data);db.session.commit()
            ids=SimpleNamespace(org=data.org.id,issue=data.issue.id,provider=p.id,
                manager=data.users["manager"].id,operator=data.users["receptionist"].id,
                provider_user=data.users["provider"].id)
            db.session.remove()
        yield app,engine,ids
    finally:
        db.session.remove();db.session=original;engine.dispose()
        with control.begin() as conn:conn.exec_driver_sql('DROP SCHEMA "'+schema+'" CASCADE')
        control.dispose()


def parallel(app,callbacks):
    barrier=Barrier(len(callbacks))
    def run(callback):
        with app.app_context():
            try:
                barrier.wait(timeout=5)
                row=callback()
                result=(200,row.id)
                db.session.commit()
                return result
            except HTTPException as exc:
                db.session.rollback()
                return exc.code,None
            finally:db.session.remove()
    with ThreadPoolExecutor(max_workers=len(callbacks)) as pool:
        futures=[pool.submit(run,cb) for cb in callbacks]
        return [f.result(timeout=20) for f in futures]


@pytest.mark.parametrize("same_request",[False,True])
def test_concurrent_creation_uniqueness_and_idempotency(concurrent_db,same_request):
    app,engine,i=concurrent_db
    keys=[key(),key()]
    if same_request:keys[1]=keys[0]
    callbacks=[lambda k=k:work_orders.create(i.org,i.manager,i.issue,i.provider,"Scope","Gate",k) for k in keys]
    results=parallel(app,callbacks)
    assert sorted(r[0] for r in results)==([200,200] if same_request else [200,409])
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM uip_work_order").scalar()==1
        assert conn.exec_driver_sql("SELECT count(*) FROM uip_work_order_action").scalar()==1
        assert conn.exec_driver_sql("SELECT count(*) FROM uip_audit_event WHERE action='work_order.created'").scalar()==1


def make_verified(app,i,task=False):
    with app.app_context():
        row=work_orders.create(i.org,i.manager,i.issue,i.provider,"Scope","Gate",key())
        for action,actor in [("dispatched",i.manager),("accepted",i.provider_user),("started",i.provider_user),("completed",i.provider_user),("verified",i.manager)]:
            work_orders.transition(i.org,actor,row.id,action,row.version,key(),dispatch_method="EMAIL" if action=="dispatched" else None)
        tid=operations.add_task(i.org,i.manager,i.issue,"Follow-up").id if task else None
        db.session.commit();result=(row.id,row.version,tid);db.session.remove();return result


def test_concurrent_transitions_only_one_version_wins(concurrent_db):
    app,engine,i=concurrent_db
    oid,version,_=make_verified(app,i)
    results=parallel(app,[lambda:work_orders.transition(i.org,i.manager,oid,"closed",version,key()) for _ in range(2)])
    assert sorted(r[0] for r in results)==[200,409]
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM uip_work_order_action WHERE action='closed'").scalar()==1
        assert conn.exec_driver_sql("SELECT status FROM core_interaction WHERE id="+str(i.issue)).scalar()=="RESOLVED"


def test_closure_and_new_internal_task_share_issue_lock(concurrent_db):
    app,engine,i=concurrent_db;oid,version,_=make_verified(app,i)
    results=parallel(app,[lambda:work_orders.transition(i.org,i.manager,oid,"closed",version,key()),
                          lambda:operations.add_task(i.org,i.operator,i.issue,"Concurrent follow-up")])
    assert sorted(r[0] for r in results)==[200,409]
    with engine.connect() as conn:
        closed=conn.exec_driver_sql("SELECT status FROM core_interaction WHERE id="+str(i.issue)).scalar()=="RESOLVED"
        tasks=conn.exec_driver_sql("SELECT count(*) FROM core_task").scalar()
        assert (closed and tasks==0) or (not closed and tasks==1)


def test_cancellation_and_closure_never_resolve_actionable_task(concurrent_db):
    app,engine,i=concurrent_db;oid,version,tid=make_verified(app,i,task=True)
    results=parallel(app,[lambda:work_orders.transition(i.org,i.manager,oid,"closed",version,key()),
        lambda:operations.finish_task(i.org,i.operator,tid,cancel=True,reason="No longer required",expected=1)])
    assert results[1][0]==200 and results[0][0] in (200,409)
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT status FROM core_task").scalar()=="cancelled"
        assert conn.exec_driver_sql("SELECT count(*) FROM uip_audit_event WHERE action='task.cancelled'").scalar()==1
