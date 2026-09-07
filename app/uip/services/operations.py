"""CoreTask remains the internal workflow. Unknown historical states are actionable."""
from datetime import datetime, timezone
from flask import abort
from app.extensions import db
from app.models.core import CoreInteraction, CoreTask
from app.models.uip import UipWorkOrder
from . import audit
from .providers import STAFF


def issue(org, issue_id):
    return CoreInteraction.query.filter_by(id=issue_id, organization_id=org).populate_existing().with_for_update().first_or_404()


def open_issue(ix):
    if ix.status == "RESOLVED":
        abort(409, description="The issue is already resolved.")


def actionable(task):
    return not (task.status == "completed" or (task.status == "cancelled" and
        task.uip_cancelled_at and task.uip_terminal_actor_id and task.uip_cancellation_reason))


def ensure_no_tasks(ix):
    if any(actionable(task) for task in CoreTask.query.filter_by(interaction_id=ix.id).populate_existing().all()):
        abort(409, description="Outstanding actionable internal tasks prevent closure.")


def add_task(org, actor, issue_id, title, description=None, assignee=None):
    audit.authorize(org, actor, STAFF)
    ix = issue(org, issue_id)
    open_issue(ix)
    title = (title or "").strip()
    if not title or len(title) > 255 or len(description or "") > 4000:
        abort(400, description="Invalid task details.")
    if assignee:
        audit.authorize(org, assignee, STAFF)
    task = CoreTask(interaction_id=ix.id, title=title, description=description,
                    assignee_id=assignee, status="pending", uip_version=1)
    db.session.add(task)
    if ix.status == "NEW":
        ix.status = "IN_PROGRESS"
    audit.record(org, actor, "task.created", task)
    return task


def finish_task(org, actor, task_id, cancel=False, reason=None, expected=None):
    audit.authorize(org, actor, STAFF)
    task = CoreTask.query.join(CoreInteraction).filter(CoreTask.id == task_id,
        CoreInteraction.organization_id == org).first_or_404()
    ix = issue(org, task.interaction_id)
    task = CoreTask.query.filter_by(id=task.id, interaction_id=ix.id).populate_existing().with_for_update().one()
    open_issue(ix)
    if expected is not None:
        try:
            if int(expected) != (task.uip_version or 0):
                abort(409, description="Task changed; reload.")
        except (ValueError, TypeError):
            abort(400)
    if not actionable(task):
        abort(409, description="Task is already non-actionable.")
    reason = (reason or "").strip()
    if cancel and (not reason or len(reason) > 2000):
        abort(400, description="Cancellation requires a reason of at most 2000 characters.")
    task.status = "cancelled" if cancel else "completed"
    task.uip_terminal_actor_id = actor
    task.uip_version = (task.uip_version or 0) + 1
    if cancel:
        task.uip_cancelled_at = datetime.now(timezone.utc)
        task.uip_cancellation_reason = reason
    else:
        task.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    audit.record(org, actor, "task.cancelled" if cancel else "task.completed", task,
                 {"new_state": task.status, "version": task.uip_version})
    return task


def resolve(org, actor, issue_id, verified_order=None):
    audit.authorize(org, actor, STAFF if verified_order is None else ("manager",))
    ix = issue(org, issue_id)
    open_issue(ix)
    ensure_no_tasks(ix)
    orders = UipWorkOrder.query.filter_by(organization_id=org, interaction_id=ix.id).all()
    if verified_order is None and orders:
        abort(409, description="Provider work must be verified and closed through its work order.")
    if verified_order is not None and (verified_order.interaction_id != ix.id or
        verified_order.organization_id != org or verified_order.status != "VERIFIED" or
        any(o.id != verified_order.id and o.status not in {"CLOSED","CANCELLED","REJECTED","FAILED"} for o in orders)):
        abort(409, description="A verified work order is required.")
    ix.status, ix.closed_by, ix.closed_at = "RESOLVED", actor, datetime.now(timezone.utc).replace(tzinfo=None)
    audit.record(org, actor, "interaction.resolved", ix)
    return ix
