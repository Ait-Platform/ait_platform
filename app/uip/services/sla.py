"""Elapsed UTC SLA targets. Policies are explicit; existing clocks retain their basis.

Issue acknowledgement/dispatch start at intake. Acceptance starts at dispatch,
commencement at acceptance, completion at commencement, closure at completion.
No pause is inferred. Terminal unsuccessful orders stop their remaining timers,
retaining any breach at the stop time. Historical records are not backfilled.
Caller owns the transaction, including the audit event.
"""
from datetime import datetime, timedelta, timezone
from flask import abort
from app.extensions import db
from app.models.core import CoreOrganization
from app.models.uip import UipSlaPolicy, UipSlaClock
from . import audit, providers, operations

STAGES = ("acknowledgement", "dispatch", "acceptance", "commencement", "completion", "closure")


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def configure(org, actor, category, priority, stage, minutes, warning):
    audit.authorize(org, actor, ("manager",))
    if category not in providers.CATEGORIES or priority not in {"LOW", "NORMAL", "HIGH", "URGENT"} or stage not in STAGES:
        abort(400, description="Choose an issue category, priority and SLA stage.")
    try:
        minutes, warning = int(minutes), int(warning)
    except (ValueError, TypeError):
        abort(400)
    if not 0 <= warning <= minutes or not 0 < minutes <= 5256000:
        abort(400, description="Invalid target or warning lead time.")
    CoreOrganization.query.filter_by(id=org).with_for_update().one()
    UipSlaPolicy.query.filter_by(organization_id=org, category=category, priority=priority,
                                stage=stage, is_active=True).update({"is_active": False})
    policy = UipSlaPolicy(organization_id=org, category=category, priority=priority,
        stage=stage, target_minutes=minutes, warning_minutes=warning, created_by=actor, is_active=True)
    db.session.add(policy)
    audit.record(org, actor, "sla.configured", policy)
    return policy


def start(issue, stage, at, order=None):
    policy = UipSlaPolicy.query.filter_by(organization_id=issue.organization_id,
        category=issue.category, priority=issue.priority, stage=stage, is_active=True).first()
    if not policy:
        return None
    query = UipSlaClock.query.filter_by(organization_id=issue.organization_id,
        interaction_id=issue.id, work_order_id=order.id if order else None, stage=stage)
    existing = query.first()
    if existing:
        return existing
    at = utc(at)
    target = at + timedelta(minutes=policy.target_minutes)
    clock = UipSlaClock(organization_id=issue.organization_id, interaction_id=issue.id,
        work_order_id=order.id if order else None, policy_id=policy.id, stage=stage,
        started_at=at, target_at=target, warning_at=target - timedelta(minutes=policy.warning_minutes))
    db.session.add(clock)
    return clock


def finish(issue, stage, at, order=None):
    clock = UipSlaClock.query.filter_by(organization_id=issue.organization_id,
        interaction_id=issue.id, work_order_id=order.id if order else None, stage=stage).first()
    if clock and clock.finished_at is None and clock.stopped_at is None:
        clock.finished_at = utc(at)


def intake(issue):
    for stage in ("acknowledgement", "dispatch"):
        start(issue, stage, issue.created_at)


def acknowledge(org, actor, issue_id):
    audit.authorize(org, actor, providers.STAFF)
    issue = operations.issue(org, issue_id)
    operations.open_issue(issue)
    # An audit event records the actual action even when no policy exists.
    from app.models.uip import UipAuditEvent
    if UipAuditEvent.query.filter_by(organization_id=org, entity_type="CoreInteraction",
                                    entity_id=issue.id, action="interaction.acknowledged").first():
        abort(409, description="Acknowledgement is already recorded.")
    finish(issue, "acknowledgement", datetime.now(timezone.utc))
    audit.record(org, actor, "interaction.acknowledged", issue)


def order_action(issue, order, action, at):
    if action == "dispatched":
        finish(issue, "dispatch", at)
        start(issue, "acceptance", at, order)
    elif action in {"accepted", "started", "completed"}:
        previous, following = {"accepted": ("acceptance", "commencement"),
            "started": ("commencement", "completion"), "completed": ("completion", "closure")}[action]
        finish(issue, previous, at, order)
        start(issue, following, at, order)
    elif action == "closed":
        finish(issue, "closure", at, order)
    elif action in {"cancelled", "failed", "rejected"}:
        for clock in UipSlaClock.query.filter_by(organization_id=issue.organization_id,
                work_order_id=order.id, finished_at=None, stopped_at=None).all():
            clock.stopped_at, clock.stop_reason = utc(at), action


def state(clock, now=None):
    now = utc(now or datetime.now(timezone.utc))
    target = utc(clock.target_at)
    if clock.finished_at is not None:
        return "met" if utc(clock.finished_at) <= target else "completed late"
    if clock.stopped_at is not None:
        return "stopped breached" if utc(clock.stopped_at) > target else "stopped"
    if now > target:
        return "breached"
    if now >= utc(clock.warning_at):
        return "approaching breach"
    return "within SLA"


def overview(org, actor):
    audit.authorize(org, actor, providers.STAFF)
    now = datetime.now(timezone.utc)
    return [(clock, state(clock, now)) for clock in
            UipSlaClock.query.filter_by(organization_id=org).order_by(UipSlaClock.target_at).all()]
