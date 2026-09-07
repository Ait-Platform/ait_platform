import uuid
from bootstrap import db, core, uip
from app.uip.services import providers, work_orders


def key():
    return str(uuid.uuid4())


def provider(data):
    data.issue.category = "SECURITY"
    row = providers.save(data.org.id, data.users["manager"].id,
        dict(name="Test provider", availability="AVAILABLE"), ["SECURITY"])
    membership = core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id,
        user_id=data.users["provider"].id).one()
    providers.associate(data.org.id, data.users["manager"].id, row.id, membership.id, row.version)
    db.session.flush()
    return row


def order(data, row=None):
    row = row or provider(data)
    result = work_orders.create(data.org.id, data.users["manager"].id, data.issue.id,
                               row.id, "Approved scope", "Public entrance", key())
    return result


def act(data, row, action, actor=None, **kw):
    actor = actor or ("provider" if work_orders.TRANSITIONS[action][2] == ("provider",) else "manager")
    return work_orders.transition(data.org.id, data.users[actor].id, row.id, action,
        kw.pop("expected_version", row.version), kw.pop("request_key", key()), **kw)


def completed(data):
    row=order(data)
    act(data,row,"dispatched",dispatch_method="TELEPHONE")
    act(data,row,"accepted")
    act(data,row,"started")
    act(data,row,"completed",note="Work completed without disclosing member data")
    return row
