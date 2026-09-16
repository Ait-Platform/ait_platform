"""Recorded, organisation-scoped operational counts; no SLA inference."""
from app.extensions import db
from app.models.core import CoreInteraction
from app.models.uip import UipWorkOrder
from . import audit, providers


def metrics(org, actor):
    audit.authorize(org, actor, providers.STAFF)
    counts = dict(db.session.query(UipWorkOrder.status, db.func.count(UipWorkOrder.id)).filter(
        UipWorkOrder.organization_id == org).group_by(UipWorkOrder.status).all())
    active = db.session.query(UipWorkOrder.interaction_id).filter(
        UipWorkOrder.organization_id == org, UipWorkOrder.status.notin_(providers.TERMINAL))
    result = {
        "Unassigned open issues": CoreInteraction.query.filter(CoreInteraction.organization_id == org,
            db.or_(CoreInteraction.status != "RESOLVED", CoreInteraction.status.is_(None)),
            ~CoreInteraction.id.in_(active)).count(),
        "Created, not dispatched": counts.get("CREATED", 0),
        "Dispatched": counts.get("DISPATCHED", 0),
        "Accepted / in progress": counts.get("ACCEPTED", 0) + counts.get("IN_PROGRESS", 0),
        "Awaiting verification": counts.get("COMPLETED", 0),
        "Verified, awaiting closure": counts.get("VERIFIED", 0),
        "Closed work orders": counts.get("CLOSED", 0),
        "Resolved issues": CoreInteraction.query.filter_by(organization_id=org, status="RESOLVED").count(),
    }
    from datetime import datetime, timezone
    from collections import Counter
    from app.models.core import CoreTask
    from app.models.uip import (UipProvider, UipMunicipalReferral, UipFollowUp, UipCommitteeMeeting,
        UipSurvey, UipResolution, UipDecisionEvent)
    from . import sla, operations
    now = datetime.now(timezone.utc)
    clocks = sla.overview(org, actor)
    states = Counter(state for clock, state in clocks)
    result.update({
        "SLA stages within target": states["within SLA"],
        "SLA stages approaching breach": states["approaching breach"],
        "SLA stages breached": states["breached"],
        "SLA stages met": states["met"],
        "SLA stages completed late": states["completed late"],
        "Provider acceptance delays": sum(clock.stage == "acceptance" and state == "breached" for clock, state in clocks),
        "Active providers": UipProvider.query.filter_by(organization_id=org, is_active=True).count(),
        "Unavailable providers": UipProvider.query.filter_by(organization_id=org, is_active=True, availability="UNAVAILABLE").count(),
        "Open issues": CoreInteraction.query.filter(CoreInteraction.organization_id == org,
            db.or_(CoreInteraction.status != "RESOLVED", CoreInteraction.status.is_(None))).count(),
        "Rejected / failed / cancelled work orders": sum(counts.get(s, 0) for s in ("REJECTED", "FAILED", "CANCELLED")),
    })
    for stage, label in (("acknowledgement", "Recorded acknowledgement"), ("closure", "Recorded verification/closure")):
        durations = [(sla.utc(c.finished_at) - sla.utc(c.started_at)).total_seconds() / 60 for c, state in clocks
                     if c.stage == stage and c.finished_at is not None and sla.utc(c.finished_at) >= sla.utc(c.started_at)]
        result[label + " mean minutes (sample count)"] = f"{sum(durations)/len(durations):.1f} ({len(durations)})" if durations else "No recorded sample"
    closed_issues = CoreInteraction.query.filter(CoreInteraction.organization_id == org,
        CoreInteraction.status == "RESOLVED", CoreInteraction.created_at.isnot(None), CoreInteraction.closed_at.isnot(None)).all()
    closure_times = [(sla.utc(i.closed_at) - sla.utc(i.created_at)).total_seconds() / 60 for i in closed_issues
                     if sla.utc(i.closed_at) >= sla.utc(i.created_at)]
    result["Recorded issue closure mean minutes (sample count)"] = f"{sum(closure_times)/len(closure_times):.1f} ({len(closure_times)})" if closure_times else "No recorded sample"
    refs = UipMunicipalReferral.query.filter_by(organization_id=org).all()
    terminal = {"RESOLVED", "RESOLVED_BY_CITY", "CLOSED"}
    result.update({
        "Open municipal referrals": sum(r.status not in terminal for r in refs),
        "Municipal awaiting acknowledgement": sum(r.status == "SUBMITTED" for r in refs),
        "Municipal in progress": sum(r.status == "IN_PROGRESS" for r in refs),
        "Municipal overdue (recorded due date)": sum(r.status not in terminal and r.sla_expected_date is not None and sla.utc(r.sla_expected_date) < now for r in refs),
        "Municipal resolved / closed": sum(r.status in terminal for r in refs),
    })
    follows = UipFollowUp.query.filter_by(organization_id=org, completed_at=None).all()
    tasks = CoreTask.query.join(CoreInteraction).filter(CoreInteraction.organization_id == org).all()
    result["Outstanding follow-ups"] = sum(f.next_action != "NONE" for f in follows)
    result["Unresolved contact attempts"] = sum(f.outcome in {"NO_ANSWER", "UNREACHABLE"} for f in follows)
    result["Overdue internal tasks"] = sum(operations.actionable(t) and t.due_date is not None and sla.utc(t.due_date) < now for t in tasks)
    result["Upcoming meetings"] = UipCommitteeMeeting.query.filter(UipCommitteeMeeting.organization_id == org,
        UipCommitteeMeeting.status == "SCHEDULED", UipCommitteeMeeting.scheduled_at >= now.replace(tzinfo=None)).count()
    result["Meetings with quorum achieved"] = UipCommitteeMeeting.query.filter_by(organization_id=org, quorum_achieved=True).count()
    result["Meetings with quorum not achieved"] = UipCommitteeMeeting.query.filter_by(organization_id=org, quorum_achieved=False).count()
    result["Open surveys"] = UipSurvey.query.filter(UipSurvey.organization_id == org, UipSurvey.status == "OPEN",
        UipSurvey.opens_at <= now, UipSurvey.closes_at > now).count()
    decision_states = {r.id: r.status for r in UipResolution.query.filter_by(organization_id=org).all()}
    for event in UipDecisionEvent.query.filter_by(organization_id=org).order_by(UipDecisionEvent.id).all():
        decision_states[event.decision_id] = event.status
    result["Unresolved governance decisions"] = sum(s not in {"COMPLETED", "SUPERSEDED", "EXECUTED", "REJECTED"} for s in decision_states.values())
    for column, prefix in ((CoreInteraction.priority, "Issues by priority"), (CoreInteraction.category, "Issues by category")):
        for value, count in db.session.query(column, db.func.count(CoreInteraction.id)).filter(
                CoreInteraction.organization_id == org).group_by(column).all():
            result[f"{prefix}: {value or 'Not recorded'}"] = count
    return result
