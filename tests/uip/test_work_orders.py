import pytest
from werkzeug.exceptions import Conflict, Forbidden, BadRequest, NotFound
from bootstrap import db, core, uip
from app.uip.services import work_orders, operations, audit, providers
from phase3_helpers import provider, order, act, completed, key


def test_complete_lifecycle_and_atomic_closure(data):
    row=completed(data)
    assert row.status == "COMPLETED" and data.issue.status != "RESOLVED"
    act(data,row,"verified")
    assert data.issue.status != "RESOLVED"
    act(data,row,"closed")
    assert row.status == "CLOSED" and data.issue.status == "RESOLVED"
    journal=uip.UipWorkOrderAction.query.filter_by(work_order_id=row.id).order_by(uip.UipWorkOrderAction.resulting_version).all()
    assert [x.new_state for x in journal] == ["CREATED","DISPATCHED","ACCEPTED","IN_PROGRESS","COMPLETED","VERIFIED","CLOSED"]
    assert [x.resulting_version for x in journal] == list(range(1,8))
    assert all(x.audit_event_id and x.actor_user_id and x.occurred_at and x.organization_id == data.org.id for x in journal)
    assert uip.UipAuditEvent.query.filter_by(action="interaction.resolved").count()==1
    assert all("Work completed" not in str(x.metadata_json) for x in uip.UipAuditEvent.query.all())


STATES={"CREATED","DISPATCHED","ACCEPTED","IN_PROGRESS","COMPLETED","VERIFIED","CLOSED","CANCELLED","REJECTED","FAILED"}
INVALID=[(state,action) for state in sorted(STATES) for action,(sources,*_) in work_orders.TRANSITIONS.items() if state not in sources]
@pytest.mark.parametrize("state,action",INVALID)
def test_every_invalid_transition(data,state,action):
    row=order(data)
    # Seed a recorded dispatch so provider visibility does not mask transition validation.
    act(data,row,"dispatched",dispatch_method="EMAIL")
    row.status=state;db.session.flush()
    before=uip.UipWorkOrderAction.query.count()
    options=dict(note="Controlled reason",reason_code="OTHER")
    if action=="dispatched": options["dispatch_method"]="EMAIL"
    with pytest.raises(Conflict): act(data,row,action,**options)
    assert row.status==state and uip.UipWorkOrderAction.query.count()==before


@pytest.mark.parametrize("end,steps",[("rejected",[]),("failed",["accepted"]),("cancelled",[])])
def test_terminal_failure_allows_new_order_without_resolving(data,end,steps):
    row=order(data);act(data,row,"dispatched",dispatch_method="TELEPHONE")
    for step in steps: act(data,row,step)
    act(data,row,end,note="Cannot undertake this work",reason_code="OTHER")
    assert data.issue.status != "RESOLVED"
    replacement=order(data,providers.get(data.org.id,row.provider_id))
    assert replacement.id != row.id and replacement.status=="CREATED"
    assert uip.UipWorkOrder.query.count()==2
    with pytest.raises(Conflict): operations.resolve(data.org.id,data.users["manager"].id,data.issue.id)


def test_verification_rejection_preserves_completion_history(data):
    row=completed(data)
    act(data,row,"verification_rejected",note="Repair incomplete",reason_code="WORK_INCOMPLETE")
    assert row.status=="IN_PROGRESS"
    act(data,row,"completed",note="Repair finished")
    assert uip.UipWorkOrderAction.query.filter_by(action="completed").count()==2
    act(data,row,"verified");act(data,row,"closed")


def test_duplicate_creation_replay_and_conflicting_payload(data):
    p=provider(data);k=key()
    args=(data.org.id,data.users["manager"].id,data.issue.id,p.id,"Approved","Entrance",k)
    first=work_orders.create(*args);second=work_orders.create(*args)
    assert first.id==second.id and uip.UipWorkOrderAction.query.count()==1
    with pytest.raises(Conflict):work_orders.create(*args[:-3],"Changed","Entrance",k)
    with pytest.raises(Conflict):order(data,p)


def test_transition_idempotency_stale_version_and_changed_replay(data):
    row=order(data);k=key();v=row.version
    act(data,row,"dispatched",request_key=k,expected_version=v,dispatch_method="EMAIL")
    act(data,row,"dispatched",request_key=k,expected_version=v,dispatch_method="EMAIL")
    assert row.version==2 and uip.UipWorkOrderAction.query.count()==2
    with pytest.raises(Conflict):act(data,row,"accepted",expected_version=v)
    with pytest.raises(Conflict):act(data,row,"dispatched",request_key=k,expected_version=v,dispatch_method="TELEPHONE")


def test_transaction_rolls_back_state_journal_audit_and_issue(data,monkeypatch):
    row=completed(data);act(data,row,"verified");db.session.flush()
    oid=row.id;before=uip.UipAuditEvent.query.count();journal=uip.UipWorkOrderAction.query.count()
    original=work_orders._journal
    def fail(*args,**kw):
        original(*args,**kw)
        raise RuntimeError("simulated failure after journal")
    monkeypatch.setattr(work_orders,"_journal",fail)
    with pytest.raises(RuntimeError), db.session.begin_nested():act(data,row,"closed")
    db.session.expire_all()
    assert db.session.get(uip.UipWorkOrder,oid).status=="VERIFIED"
    assert data.issue.status != "RESOLVED"
    assert uip.UipAuditEvent.query.count()==before and uip.UipWorkOrderAction.query.count()==journal


@pytest.mark.parametrize("cancel",[False,True])
def test_completed_or_authorised_cancelled_internal_task_permits_closure(data,cancel):
    row=completed(data)
    task=operations.add_task(data.org.id,data.users["manager"].id,data.issue.id,"Inspect","Keep original history")
    operations.finish_task(data.org.id,data.users["receptionist"].id,task.id,cancel=cancel,reason="No longer required",expected=1)
    act(data,row,"verified");act(data,row,"closed")
    assert task.title=="Inspect" and task.description=="Keep original history"
    assert task.uip_terminal_actor_id==data.users["receptionist"].id
    if cancel:
        assert task.status=="cancelled" and task.uip_cancelled_at and task.uip_cancellation_reason=="No longer required"
        assert uip.UipAuditEvent.query.filter_by(action="task.cancelled",entity_id=task.id).count()==1
    assert data.issue.status=="RESOLVED"


@pytest.mark.parametrize("status",["pending","in_progress","unknown","cancelled",None])
def test_actionable_and_unverified_historical_cancelled_tasks_block(data,status):
    row=completed(data);act(data,row,"verified")
    task=core.CoreTask(interaction_id=data.issue.id,title="History",status=status)
    db.session.add(task);db.session.flush()
    if status is None:task.status=None;db.session.flush()
    with pytest.raises(Conflict):act(data,row,"closed")
    assert row.status=="VERIFIED" and data.issue.status!="RESOLVED"


@pytest.mark.parametrize("actor",["provider","resident","committee_member","owner"])
def test_unauthorised_task_cancellation_denied(data,actor):
    task=operations.add_task(data.org.id,data.users["manager"].id,data.issue.id,"Keep")
    with pytest.raises(Forbidden):operations.finish_task(data.org.id,data.users[actor].id,task.id,cancel=True,reason="Bad")
    assert task.status=="pending"
    assert uip.UipAuditEvent.query.filter_by(action="task.cancelled").count()==0


def test_cancellation_reason_version_rollback_and_terminal_history(data,monkeypatch):
    task=operations.add_task(data.org.id,data.users["manager"].id,data.issue.id,"Keep")
    with pytest.raises(BadRequest):operations.finish_task(data.org.id,data.users["manager"].id,task.id,cancel=True)
    with pytest.raises(Conflict):operations.finish_task(data.org.id,data.users["manager"].id,task.id,cancel=True,reason="Reason",expected=0)
    with pytest.raises(RuntimeError),db.session.begin_nested():
        operations.finish_task(data.org.id,data.users["manager"].id,task.id,cancel=True,reason="No longer needed")
        raise RuntimeError("rollback")
    assert task.status=="pending" and task.uip_cancelled_at is None
    operations.finish_task(data.org.id,data.users["manager"].id,task.id,cancel=True,reason="No longer needed")
    with pytest.raises(Conflict):operations.finish_task(data.org.id,data.users["manager"].id,task.id)


def test_resolved_issue_cannot_gain_tasks_or_orders(data):
    p=provider(data);operations.resolve(data.org.id,data.users["manager"].id,data.issue.id)
    with pytest.raises(Conflict):order(data,p)
    with pytest.raises(Conflict):operations.add_task(data.org.id,data.users["manager"].id,data.issue.id,"No")


def test_journal_is_append_only(data):
    import sqlalchemy as sa
    row=order(data)
    for sql in ("UPDATE uip_work_order_action SET note='changed'", "DELETE FROM uip_work_order_action"):
        with pytest.raises(sa.exc.DBAPIError),db.session.begin_nested():db.session.execute(sa.text(sql))
    assert uip.UipWorkOrderAction.query.count()==1
