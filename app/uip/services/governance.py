"""Dated register eligibility, frozen quorum, confidential polls and decisions.

All mutators leave transaction ownership with the caller. Governance privileges
permit administration, never an eligibility shortcut for attendance or voting.
"""
import math
from datetime import datetime, timezone, date
from flask import abort
from app.extensions import db
from app.models.core import CoreOrganization, CoreInteraction, CoreTask
from app.models.uip import (UipMemberProfile, UipProperty, UipPropertyMember,
    UipMemberRepresentative, UipCommitteeMeeting, UipResolution)
from app.models.uip_governance import (UipQuorumRule, UipMeetingParticipant, UipSurvey,
    UipSurveyResponse, UipDecisionEvent)
from . import audit, operations
from .reception import text, timestamp, identifier
from .sla import utc

ADMIN = ("manager", "committee_member")
READ = ("manager", "committee_member", "owner", "resident")


def eligibility(org, as_of, relationship):
    """One eligible registered member, even when they own multiple properties."""
    if relationship not in {"owner", "occupier"}:
        abort(400, description="Configure the participation relationship.")
    members = {m.id: m for m in UipMemberProfile.query.filter_by(organization_id=org,
        is_active=True).all()}
    properties = {p.id for p in UipProperty.query.filter_by(organization_id=org, is_active=True).all()}
    result = {}
    for link in UipPropertyMember.query.filter(UipPropertyMember.organization_id == org,
        UipPropertyMember.is_verified.is_(True), UipPropertyMember.relationship == relationship,
        UipPropertyMember.valid_from <= as_of,
        db.or_(UipPropertyMember.valid_to.is_(None), UipPropertyMember.valid_to >= as_of)).all():
        member = members.get(link.member_id)
        if not member or member.eligibility_status != "eligible" or link.property_id not in properties:
            continue
        entry = result.setdefault(str(member.id), dict(member_id=member.id,
            as_of=as_of.isoformat(), relationship=relationship, eligibility_status=member.eligibility_status,
            is_active=member.is_active, property_links=[], representatives=[]))
        entry["property_links"].append(dict(id=link.id, property_id=link.property_id,
            valid_from=link.valid_from.isoformat(), valid_to=link.valid_to.isoformat() if link.valid_to else None))
    for link in UipMemberRepresentative.query.filter(UipMemberRepresentative.organization_id == org,
        UipMemberRepresentative.is_verified.is_(True), UipMemberRepresentative.valid_from <= as_of,
        db.or_(UipMemberRepresentative.valid_to.is_(None), UipMemberRepresentative.valid_to >= as_of)).all():
        if str(link.member_id) in result and link.representative_id in members:
            result[str(link.member_id)]["representatives"].append(dict(id=link.id,
                member_id=link.representative_id, valid_from=link.valid_from.isoformat(),
                valid_to=link.valid_to.isoformat() if link.valid_to else None))
    return result


def quorum_rule(org, actor, percentage, minimum, relationship):
    audit.authorize(org, actor, ADMIN)
    try:
        percentage, minimum = int(percentage), int(minimum)
    except (ValueError, TypeError):
        abort(400)
    if not 1 <= percentage <= 100 or minimum < 1 or relationship not in {"owner", "occupier"}:
        abort(400, description="Invalid quorum rule.")
    CoreOrganization.query.filter_by(id=org).with_for_update().one()
    rule = UipQuorumRule.query.filter_by(organization_id=org).first()
    if not rule:
        rule = UipQuorumRule(organization_id=org)
        db.session.add(rule)
    rule.percentage, rule.minimum, rule.relationship = percentage, minimum, relationship
    rule.updated_by, rule.updated_at = actor, datetime.now(timezone.utc)
    audit.record(org, actor, "quorum.configured", rule)
    return rule


def meeting(org, actor, values):
    audit.authorize(org, actor, ADMIN)
    row = UipCommitteeMeeting(organization_id=org, title=text(values.get("title"), 255, True),
        meeting_type=text(values.get("meeting_type"), 50, True),
        scheduled_at=timestamp(values.get("scheduled_at"), future=True).replace(tzinfo=None),
        location=text(values.get("location"), 255, True), agenda=text(values.get("agenda"), 20000, True), status="SCHEDULED")
    if not values.get("scheduled_at"):
        abort(400, description="A meeting date is required.")
    db.session.add(row)
    audit.record(org, actor, "meeting.created", row)
    return row


def update_meeting(org, actor, meeting_id, values, cancel=False):
    audit.authorize(org, actor, ADMIN)
    row = UipCommitteeMeeting.query.filter_by(organization_id=org, id=meeting_id).populate_existing().with_for_update().first_or_404()
    if row.status != "SCHEDULED" or row.eligibility_basis is not None:
        abort(409, description="Only a scheduled meeting can be edited or cancelled. Started/concluded meeting history is retained.")
    if cancel:
        row.status = "CANCELLED"
        row.minutes_text = text(values.get("reason"), 4000, True)
        action = "meeting.cancelled"
    else:
        row.title = text(values.get("title"), 255, True)
        row.meeting_type = text(values.get("meeting_type"), 50, True)
        if not values.get("scheduled_at"):
            abort(400, description="A scheduled date is required.")
        row.scheduled_at = timestamp(values.get("scheduled_at"), future=True).replace(tzinfo=None)
        row.location = text(values.get("location"), 255, True)
        row.agenda = text(values.get("agenda"), 20000, True)
        action = "meeting.updated"
    audit.record(org, actor, action, row)
    return row


def start_meeting(org, actor, meeting_id):
    audit.authorize(org, actor, ADMIN)
    row = UipCommitteeMeeting.query.filter_by(organization_id=org, id=meeting_id).populate_existing().with_for_update().first_or_404()
    if row.status != "SCHEDULED" or row.eligibility_basis is not None:
        abort(409)
    rule = UipQuorumRule.query.filter_by(organization_id=org).first()
    if not rule:
        abort(409, description="Configure quorum before starting the meeting.")
    now = datetime.now(timezone.utc)
    if utc(row.scheduled_at) > now:
        abort(409, description="The meeting has not reached its scheduled time.")
    row.eligibility_basis = eligibility(org, now.date(), rule.relationship)
    row.quorum_rule = dict(percentage=rule.percentage, minimum=rule.minimum,
                          relationship=rule.relationship, captured_at=now.isoformat())
    row.status = "IN_PROGRESS"
    audit.record(org, actor, "meeting.started", row)
    return row


def attendance(org, actor, meeting_id, member_id, status, proxy_id=None):
    audit.authorize(org, actor, ADMIN)
    member_id = identifier(member_id)
    proxy_id = identifier(proxy_id) if proxy_id else None
    meeting_row = UipCommitteeMeeting.query.filter_by(organization_id=org, id=meeting_id).populate_existing().with_for_update().first_or_404()
    if meeting_row.status not in {"SCHEDULED", "IN_PROGRESS"}:
        abort(409, description="Attendance is frozen after the meeting concludes.")
    if status not in {"INVITED", "PRESENT", "APOLOGY", "ABSENT"}:
        abort(400)
    if status == "PRESENT" and meeting_row.status != "IN_PROGRESS":
        abort(409, description="Start the meeting before recording attendance.")
    member = UipMemberProfile.query.filter_by(organization_id=org, id=member_id).first_or_404()
    proxy = UipMemberProfile.query.filter_by(organization_id=org, id=proxy_id).first_or_404() if proxy_id else member
    basis = (meeting_row.eligibility_basis or {}).get(str(member.id))
    if proxy.id != member.id and (not basis or proxy.id not in {r["member_id"] for r in basis["representatives"]}):
        abort(403, description="No verified dated representation exists in the meeting basis.")
    row = UipMeetingParticipant.query.filter_by(organization_id=org, meeting_id=meeting_id, member_id=member.id).first()
    if not row:
        row = UipMeetingParticipant(organization_id=org, meeting_id=meeting_id, member_id=member.id)
        db.session.add(row)
    row.status, row.attended_by_member_id = status, proxy.id if status == "PRESENT" else None
    row.recorded_by, row.recorded_at = actor, datetime.now(timezone.utc)
    audit.record(org, actor, "meeting.attendance", row)
    return row


def conclude(org, actor, meeting_id, minutes):
    audit.authorize(org, actor, ADMIN)
    row = UipCommitteeMeeting.query.filter_by(organization_id=org, id=meeting_id).populate_existing().with_for_update().first_or_404()
    if row.status != "IN_PROGRESS" or row.quorum_rule is None:
        abort(409)
    basis = row.eligibility_basis
    row.eligible_count = len(basis)
    row.attendance_count = sum(str(p.member_id) in basis for p in UipMeetingParticipant.query.filter_by(
        organization_id=org, meeting_id=row.id, status="PRESENT").all())
    row.required_quorum = max(row.quorum_rule["minimum"], math.ceil(row.eligible_count * row.quorum_rule["percentage"] / 100))
    row.quorum_achieved = row.attendance_count >= row.required_quorum
    row.minutes_text, row.status = text(minutes, 50000, True), "CONCLUDED"
    row.quorum_recorded_at, row.quorum_recorded_by = datetime.now(timezone.utc), actor
    audit.record(org, actor, "meeting.concluded", row)
    return row


def survey(org, actor, values, questions):
    audit.authorize(org, actor, ADMIN)
    if not isinstance(questions, list) or not 1 <= len(questions) <= 30:
        abort(400, description="Supply between 1 and 30 questions.")
    clean = []
    for n, question in enumerate(questions):
        if not isinstance(question, dict) or question.get("type") not in {"SINGLE_CHOICE", "YES_NO", "RATING"}:
            abort(400, description="Use single choice, yes/no or a 1–5 rating.")
        options = question.get("options") if question["type"] == "SINGLE_CHOICE" else (
            ["Yes", "No"] if question["type"] == "YES_NO" else ["1", "2", "3", "4", "5"])
        if not isinstance(options, list) or not 2 <= len(options) <= 20:
            abort(400)
        options = [text(o, 200, True) for o in options]
        if len(set(options)) != len(options):
            abort(400, description="Answer options must be distinct.")
        clean.append(dict(id=str(n + 1), title=text(question.get("title"), 500, True), type=question["type"], options=options))
    relationship = values.get("relationship")
    if relationship not in {"owner", "occupier"} or not values.get("opens_at") or not values.get("closes_at"):
        abort(400)
    opens, closes = timestamp(values["opens_at"], True), timestamp(values["closes_at"], True)
    if closes <= opens or closes <= datetime.now(timezone.utc):
        abort(400, description="The closing date must follow opening and be in the future.")
    identifiable = values.get("identifiable")
    if identifiable not in {"yes", "no"}:
        abort(400, description="Choose whether responses are identifiable.")
    row = UipSurvey(organization_id=org, title=text(values.get("title"), 255, True),
        purpose=text(values.get("purpose"), 4000, True), opens_at=opens, closes_at=closes,
        relationship=relationship, identifiable=identifiable == "yes", questions=clean,
        status="OPEN", created_by=actor)
    row.eligibility_snapshot = eligibility(org, opens.date(), relationship)
    db.session.add(row)
    audit.record(org, actor, "survey.created", row)
    return row


def respond(org, actor, survey_id, member_id, answers):
    audit.authorize(org, actor, READ)
    member_id = identifier(member_id)
    row = UipSurvey.query.filter_by(organization_id=org, id=survey_id).populate_existing().with_for_update().first_or_404()
    basis = response_basis(row, member_id)
    allowed_members = [basis["member_id"]] + [r["member_id"] for r in basis["representatives"]]
    # The logged-in actor must be the member or their dated verified representative.
    candidates = UipMemberProfile.query.filter(UipMemberProfile.organization_id == org,
                                              UipMemberProfile.id.in_(allowed_members)).all()
    if not any(m.membership.user_id == actor for m in candidates):
        abort(403)
    return store_response(row, actor, member_id, answers, basis)


def response_basis(row, member_id):
    now = datetime.now(timezone.utc)
    if row.status != "OPEN" or not utc(row.opens_at) <= now < utc(row.closes_at):
        abort(409, description="This survey is not open for responses.")
    if row.eligibility_snapshot is None:
        row.eligibility_snapshot = eligibility(row.organization_id, now.date(), row.relationship)
    basis = row.eligibility_snapshot.get(str(member_id))
    if not basis:
        abort(403, description="This member is not in the survey eligibility snapshot.")
    return basis


def store_response(row, actor, member_id, answers, basis):
    """Internal common ballot writer; callers authenticate login or invitation first."""
    org = row.organization_id
    if UipSurveyResponse.query.filter_by(organization_id=org, survey_id=row.id, member_id=member_id).first():
        abort(409, description="A response is already recorded for this eligible member.")
    if not isinstance(answers, dict) or set(answers) != {q["id"] for q in row.questions}:
        abort(400, description="Answer every question once.")
    for question in row.questions:
        if answers[question["id"]] not in question["options"]:
            abort(400, description="Choose an allowed answer.")
    response = UipSurveyResponse(organization_id=org, survey_id=row.id, member_id=member_id,
        actor_user_id=actor, eligibility_basis=basis, answers=answers, responded_at=datetime.now(timezone.utc))
    db.session.add(response)
    if actor is not None:
        audit.record(org, actor, "survey.responded", row)
    else:
        from app.models.uip import UipAuditEvent
        db.session.add(UipAuditEvent(organization_id=org, actor_user_id=None, action="survey.responded",
            entity_type="UipSurvey", entity_id=row.id, metadata_json={"source": "secure_invitation"}))
    return response


def finalize(org, actor, survey_id):
    audit.authorize(org, actor, ADMIN)
    row = UipSurvey.query.filter_by(organization_id=org, id=survey_id).populate_existing().with_for_update().first_or_404()
    if row.status != "OPEN" or datetime.now(timezone.utc) < utc(row.closes_at):
        abort(409, description="Results can be finalized only after the closing time.")
    responses = UipSurveyResponse.query.filter_by(organization_id=org, survey_id=row.id).all()
    results = {q["id"]: {option: 0 for option in q["options"]} for q in row.questions}
    for response in responses:
        for key, answer in response.answers.items():
            results[key][answer] += 1
    row.results = dict(response_count=len(responses), questions=results)
    row.status, row.finalized_at, row.finalized_by = "FINALIZED", datetime.now(timezone.utc), actor
    audit.record(org, actor, "survey.finalized", row)
    return row


def decision(org, actor, values):
    audit.authorize(org, actor, ADMIN)
    meeting_id, survey_id = values.get("meeting_id"), values.get("survey_id")
    if bool(meeting_id) == bool(survey_id):
        abort(400, description="Choose exactly one source meeting or survey.")
    meeting_id = identifier(meeting_id) if meeting_id else None
    survey_id = identifier(survey_id) if survey_id else None
    if meeting_id:
        source = UipCommitteeMeeting.query.filter_by(organization_id=org, id=meeting_id).first_or_404()
        if source.status != "CONCLUDED" or source.quorum_achieved is not True:
            abort(409, description="A concluded meeting with recorded quorum is required.")
        basis = dict(eligible_count=source.eligible_count, attendance_count=source.attendance_count,
                     required_quorum=source.required_quorum, quorum_rule=source.quorum_rule)
        try:
            votes = {key: int(values.get(key)) for key in ("votes_for", "votes_against", "abstentions")}
        except (ValueError, TypeError):
            abort(400, description="Record the meeting vote counts, including zeroes.")
        if any(v < 0 for v in votes.values()) or sum(votes.values()) > source.attendance_count:
            abort(400, description="Vote totals cannot exceed the eligible attendance.")
        basis["votes"] = votes
    else:
        source = UipSurvey.query.filter_by(organization_id=org, id=survey_id).first_or_404()
        if source.status != "FINALIZED":
            abort(409, description="Finalize the survey before recording a decision.")
        basis = source.results
    task_id = values.get("linked_task_id")
    if task_id:
        task_id = identifier(task_id)
        task_id = CoreTask.query.join(CoreInteraction).filter(CoreTask.id == task_id,
            CoreInteraction.organization_id == org).first_or_404().id
    old = None
    responsible = identifier(values.get("responsible_user_id") or actor)
    audit.authorize(org, responsible, ("manager", "committee_member", "receptionist"))
    if values.get("supersedes_id"):
        old = UipResolution.query.filter_by(organization_id=org, id=identifier(values["supersedes_id"])).populate_existing().with_for_update().first_or_404()
        if UipResolution.query.filter_by(organization_id=org, supersedes_id=old.id).first():
            abort(409, description="That decision already has a superseding decision.")
    row = UipResolution(organization_id=org, meeting_id=source.id if meeting_id else None,
        survey_id=source.id if survey_id else None, title=text(values.get("title"), 255, True),
        description=text(values.get("description"), 20000, True), status="RECORDED",
        decision_date=datetime.now(timezone.utc).date(), recorded_by=actor, result_basis=basis,
        responsible_user_id=responsible,
        linked_task_id=task_id or None, supersedes_id=old.id if old else None)
    db.session.add(row)
    audit.record(org, actor, "decision.recorded", row)
    if old:
        db.session.add(UipDecisionEvent(organization_id=org, decision_id=old.id,
            actor_user_id=actor, status="SUPERSEDED", note="Superseded by decision " + str(row.id)))
    return row


def decision_status(org, actor, decision_id, status, note):
    audit.authorize(org, actor, ADMIN)
    decision_id = identifier(decision_id)
    row = UipResolution.query.filter_by(organization_id=org, id=decision_id).populate_existing().with_for_update().first_or_404()
    if status not in {"IN_PROGRESS", "COMPLETED"}:
        abort(400)
    if UipResolution.query.filter_by(organization_id=org, supersedes_id=row.id).first():
        abort(409, description="The decision has been superseded.")
    last = UipDecisionEvent.query.filter_by(organization_id=org, decision_id=row.id).order_by(UipDecisionEvent.id.desc()).first()
    if last and last.status in {"COMPLETED", "SUPERSEDED"}:
        abort(409)
    if status == "COMPLETED" and row.linked_task_id:
        task = CoreTask.query.join(CoreInteraction).filter(CoreTask.id == row.linked_task_id,
            CoreInteraction.organization_id == org).first_or_404()
        if operations.actionable(task):
            abort(409, description="Complete the linked internal task first.")
    event = UipDecisionEvent(organization_id=org, decision_id=row.id, actor_user_id=actor,
                             status=status, note=text(note, required=True))
    db.session.add(event)
    audit.record(org, actor, "decision.status_recorded", row)
    return event


def overview(org, actor):
    from app.models.uip import UipMunicipalReferral
    audit.authorize(org, actor, ADMIN)
    now = datetime.now(timezone.utc)
    states = {r.id: r.status for r in UipResolution.query.filter_by(organization_id=org).all()}
    for event in UipDecisionEvent.query.filter_by(organization_id=org).order_by(UipDecisionEvent.id).all():
        states[event.decision_id] = event.status
    return {
        "Unresolved decisions": sum(s not in {"COMPLETED", "SUPERSEDED", "EXECUTED", "REJECTED"} for s in states.values()),
        "Open municipal referrals": UipMunicipalReferral.query.filter(UipMunicipalReferral.organization_id == org,
            UipMunicipalReferral.status.notin_({"CLOSED", "RESOLVED", "RESOLVED_BY_CITY"})).count(),
        "Upcoming meetings": UipCommitteeMeeting.query.filter(UipCommitteeMeeting.organization_id == org,
            UipCommitteeMeeting.status == "SCHEDULED", UipCommitteeMeeting.scheduled_at >= now.replace(tzinfo=None)).count(),
        "Open surveys": UipSurvey.query.filter(UipSurvey.organization_id == org, UipSurvey.status == "OPEN",
            UipSurvey.opens_at <= now, UipSurvey.closes_at > now).count(),
    }

def has_delegation(org_id, user_id, delegation_type):
    from app.models.uip_governance import UipDelegation
    return UipDelegation.query.filter_by(
        organization_id=org_id, 
        delegated_user_id=user_id, 
        delegation_type=delegation_type, 
        status='ACTIVE'
    ).first() is not None
