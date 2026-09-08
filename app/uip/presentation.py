"""Read-only view models for UIP screens. Lifecycle services remain authoritative."""
from collections import Counter
from datetime import date, datetime, timezone

from app.models.core import CoreInteraction, CoreTask
from app.models.uip import (UipMemberProfile, UipProperty, UipPropertyMember, UipProvider,
    UipWorkOrder, UipFollowUp, UipMunicipalReferral,
    UipCommitteeMeeting, UipResolution, UipDecisionEvent, UipAuditEvent, UipSurvey, UipDocument)
from app.uip.services import audit, operations, providers, sla


def current_relationship(row):
    today = date.today()
    return row.valid_from <= today and (row.valid_to is None or row.valid_to >= today)


def display_value(value):
    if isinstance(value, datetime):
        return sla.utc(value).strftime("%d %b %Y · %H:%M UTC")
    if isinstance(value, date):
        return value.strftime("%d %b %Y")
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, str) and value in {"SCHEDULED", "IN_PROGRESS", "CONCLUDED", "CANCELLED", "OPEN", "FINALIZED", "CREATED", "DISPATCHED", "ACCEPTED", "COMPLETED", "VERIFIED", "CLOSED", "SUBMITTED", "ACKNOWLEDGED", "RESPONSE_RECEIVED", "RESOLVED", "UNSUCCESSFUL", "ESCALATED", "REJECTED", "FAILED"}:
        return value.replace("_", " ").title()
    return value if value is not None else "—"


def register_links(org):
    members = {m.id: m for m in UipMemberProfile.query.filter_by(organization_id=org).all()}
    properties = {p.id: p for p in UipProperty.query.filter_by(organization_id=org).all()}
    links = UipPropertyMember.query.filter_by(organization_id=org).all()
    return members, properties, links


def issue_rows(org, actor):
    audit.authorize(org, actor, providers.STAFF)
    members, properties, _ = register_links(org)
    provider_names = {p.id: p.name for p in UipProvider.query.filter_by(organization_id=org).all()}
    orders = UipWorkOrder.query.filter_by(organization_id=org).all()
    tasks = CoreTask.query.join(CoreInteraction).filter(CoreInteraction.organization_id == org).all()
    refs = UipMunicipalReferral.query.filter_by(organization_id=org).all()
    result = []
    for issue in CoreInteraction.query.filter_by(organization_id=org).order_by(
        CoreInteraction.created_at.desc().nullslast(), CoreInteraction.id.desc()).all():
        assigned = [o for o in orders if o.interaction_id == issue.id and o.status not in providers.TERMINAL]
        internal = bool(issue.assigned_to) or any(t.interaction_id == issue.id and operations.actionable(t) and t.assignee_id for t in tasks)
        waiting = any(r.interaction_id == issue.id and r.status in {"SUBMITTED", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED"} for r in refs)
        states = {o.status for o in assigned}
        filters = {"all"}
        if issue.status == "RESOLVED":
            filters.add("resolved")
        else:
            filters.add("open")
            if not assigned and not internal:
                filters.add("unassigned")
            if states & {"ACCEPTED", "IN_PROGRESS"} or issue.status == "IN_PROGRESS":
                filters.add("in_progress")
            if waiting or "DISPATCHED" in states or issue.status in {"WAITING", "ESCALATED"}:
                filters.add("waiting")
        result.append(dict(issue=issue, member=members.get(issue.member_id), property=properties.get(issue.property_id),
            assigned=", ".join(dict.fromkeys(provider_names.get(o.provider_id, "Provider") for o in assigned)) or ("Internal team" if internal else "Unassigned"),
            filters=filters))
    return result


def executive(org, actor):
    audit.authorize(org, actor, ("manager",))
    now = datetime.now(timezone.utc)
    rows = issue_rows(org, actor)
    orders = UipWorkOrder.query.filter_by(organization_id=org).all()
    tasks = CoreTask.query.join(CoreInteraction).filter(CoreInteraction.organization_id == org).all()
    follows = UipFollowUp.query.filter_by(organization_id=org, completed_at=None).all()
    refs = UipMunicipalReferral.query.filter_by(organization_id=org).all()
    clocks = sla.overview(org, actor)
    active_orders = [o for o in orders if o.status not in providers.TERMINAL]
    open_refs = [r for r in refs if r.status not in {"CLOSED", "RESOLVED", "RESOLVED_BY_CITY"}]
    actionable = [t for t in tasks if operations.actionable(t)]
    upcoming = UipCommitteeMeeting.query.filter(UipCommitteeMeeting.organization_id == org,
        UipCommitteeMeeting.status == "SCHEDULED", UipCommitteeMeeting.scheduled_at >= now.replace(tzinfo=None)).count()
    cards = [("Open issues", sum("open" in r["filters"] for r in rows), "reception_page"),
        ("Tasks / follow-ups", len(actionable) + sum(f.next_action != "NONE" for f in follows), "tasks_page"),
        ("Active work orders", len(active_orders), "work_order_list"),
        ("SLA breaches", sum(state == "breached" for c, state in clocks), "sla_page"),
        ("Open municipal matters", len(open_refs), "municipal_list"), ("Upcoming meetings", upcoming, "meetings_page")]
    attention = []
    def add(label, detail, endpoint, **params):
        attention.append(dict(label=label, detail=detail, endpoint=endpoint, params=params))
    for clock, state in clocks:
        if state in {"breached", "approaching breach"}:
            add("SLA " + state, clock.stage.replace("_", " ").title(), "reception_issue", issue_id=clock.interaction_id)
    for order in orders:
        if order.status in {"COMPLETED", "VERIFIED"}:
            add("Verify completed work" if order.status == "COMPLETED" else "Close verified work", order.reference, "work_order_view", order_id=order.id)
    for task in actionable:
        if task.due_date and sla.utc(task.due_date) < now:
            add("Overdue task", task.title, "reception_issue", issue_id=task.interaction_id)
    for follow in follows:
        if follow.next_action != "NONE" and follow.next_action_at and sla.utc(follow.next_action_at) < now:
            add("Follow-up due", follow.next_action.replace("_", " ").title(), "reception_issue", issue_id=follow.interaction_id)
    for ref in refs:
        if ref.status in {"CREATED", "SUBMITTED", "RESPONSE_RECEIVED", "UNSUCCESSFUL", "ESCALATED", "RESOLVED", "RESOLVED_BY_CITY"} or (ref in open_refs and ref.sla_expected_date and sla.utc(ref.sla_expected_date) < now):
            add("Review municipal matter", ref.department, "referral_page", referral_id=ref.id)
    decision_states = {r.id: (r, r.status) for r in UipResolution.query.filter_by(organization_id=org).all()}
    for event in UipDecisionEvent.query.filter_by(organization_id=org).order_by(UipDecisionEvent.id).all():
        if event.decision_id in decision_states:
            decision_states[event.decision_id] = (decision_states[event.decision_id][0], event.status)
    for decision, state in decision_states.values():
        if state not in {"COMPLETED", "SUPERSEDED", "EXECUTED", "REJECTED"}:
            add("Governance action open", decision.title, "decisions_page")
    for row in rows:
        if "unassigned" in row["filters"]:
            add("Assign an issue", row["issue"].title, "view_interaction", reference=row["issue"].reference)
    labels = {"interaction.created": "Interaction logged", "interaction.resolved": "Issue resolved",
        "member.created": "Ratepayer registered", "property.created": "Property added",
        "work_order.created": "Provider work arranged", "work_order.dispatched": "Work order dispatched",
        "work_order.completed": "Provider work completed", "work_order.verified": "Completed work verified",
        "referral.transitioned": "Municipal progress recorded", "follow_up.recorded": "Follow-up recorded",
        "meeting.created": "Meeting scheduled", "meeting.concluded": "Meeting concluded",
        "survey.finalized": "Survey results ready", "document.created": "Document added"}
    activity = [(labels[e.action], e.created_at) for e in UipAuditEvent.query.filter(
        UipAuditEvent.organization_id == org, UipAuditEvent.action.in_(labels)).order_by(
            UipAuditEvent.created_at.desc(), UipAuditEvent.id.desc()).limit(6).all()]
    completed_states = Counter(state for c, state in clocks if c.finished_at is not None)
    performance = [("SLA stages met", completed_states["met"]), ("SLA stages completed late", completed_states["completed late"])] if completed_states else []
    return dict(cards=cards, attention=attention, activity=activity, current_issues=[r for r in rows if "open" in r["filters"]][:8],
        performance=performance, empty=not rows and not orders and not UipMemberProfile.query.filter_by(organization_id=org).first()
        and not UipProperty.query.filter_by(organization_id=org).first() and not UipProvider.query.filter_by(organization_id=org).first()
        and not any(model.query.filter_by(organization_id=org).first() for model in (UipCommitteeMeeting, UipSurvey, UipDocument, UipResolution)))
