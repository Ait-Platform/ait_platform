"""Durable operational/security coverage using the existing disposable harness."""
import io
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import pytest
from werkzeug.datastructures import FileStorage
from werkzeug.exceptions import BadRequest, Conflict, Forbidden, NotFound
from bootstrap import db, core, uip
from app.uip.services import sla, routing, reception, governance, documents
from app.uip.services.dashboard import metrics
from phase3_helpers import provider, order, act
from test_register import make_member, make_property

BASE = "/uip/manor-gardens/operations"


def manager(data):
    return data.users["manager"].id


def configure(data, stage="acknowledgement", minutes=60, warning=10):
    data.issue.category, data.issue.priority = "SECURITY", "NORMAL"
    db.session.flush()
    return sla.configure(data.org.id, manager(data), "SECURITY", "NORMAL", stage, minutes, warning)


@pytest.mark.parametrize("offset,finished,stopped,expected", [
    (0, None, None, "within SLA"), (51, None, None, "approaching breach"),
    (61, None, None, "breached"), (100, 60, None, "met"),
    (100, 61, None, "completed late"), (100, None, 30, "stopped"),
    (100, None, 61, "stopped breached"),
])
def test_sla_recorded_states(offset, finished, stopped, expected):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    clock = SimpleNamespace(target_at=start + timedelta(minutes=60), warning_at=start + timedelta(minutes=50),
        finished_at=start + timedelta(minutes=finished) if finished is not None else None,
        stopped_at=start + timedelta(minutes=stopped) if stopped is not None else None)
    assert sla.state(clock, start + timedelta(minutes=offset)) == expected


def test_sla_configuration_does_not_backfill_history_and_targets_are_snapshotted(data):
    configure(data)
    assert uip.UipSlaClock.query.count() == 0
    sla.intake(data.issue)
    db.session.flush()
    clock = uip.UipSlaClock.query.one()
    target = clock.target_at
    configure(data, minutes=120)
    assert clock.target_at == target
    assert uip.UipSlaPolicy.query.count() == 2
    sla.acknowledge(data.org.id, manager(data), data.issue.id)
    assert clock.finished_at is not None
    with pytest.raises(Conflict):
        sla.acknowledge(data.org.id, manager(data), data.issue.id)


def test_work_order_sla_uses_actual_journal_timestamps(data):
    for stage in sla.STAGES:
        configure(data, stage)
    sla.intake(data.issue)
    row = order(data)
    for action in ("dispatched", "accepted", "started", "completed", "verified", "closed"):
        act(data, row, action, **({"dispatch_method": "EMAIL"} if action == "dispatched" else {}))
    db.session.flush()
    clocks = uip.UipSlaClock.query.filter_by(work_order_id=row.id).all()
    assert {c.stage for c in clocks} == {"acceptance", "commencement", "completion", "closure"}
    assert all(c.finished_at is not None for c in clocks)
    dispatch = uip.UipWorkOrderAction.query.filter_by(work_order_id=row.id, action="dispatched").one()
    acceptance = next(c for c in clocks if c.stage == "acceptance")
    assert sla.utc(acceptance.started_at) == sla.utc(dispatch.occurred_at)


@pytest.mark.parametrize("minutes,warning", [(0, 0), (-1, 0), (60, 61), ("bad", 1)])
def test_invalid_sla_policy_rejected(data, minutes, warning):
    with pytest.raises(BadRequest):
        configure(data, minutes=minutes, warning=warning)


def test_routing_eligibility_and_workload_are_recommendations_only(data):
    p = provider(data)
    rows = routing.recommend(data.org.id, manager(data), data.issue.id)
    assert rows[0]["provider"].id == p.id and rows[0]["active_workload"] == 0
    assert uip.UipWorkOrder.query.count() == 0
    order(data, p)
    assert routing.recommend(data.org.id, manager(data), data.issue.id)[0]["active_workload"] == 1
    p.availability = "UNAVAILABLE"
    db.session.flush()
    assert routing.recommend(data.org.id, manager(data), data.issue.id) == []


def test_followup_requires_due_date_and_keeps_actor_history(data):
    values = dict(method="TELEPHONE", outcome="NO_ANSWER", next_action="CONTACT")
    with pytest.raises(BadRequest):
        reception.follow_up(data.org.id, manager(data), data.issue.id, values)
    row = reception.follow_up(data.org.id, manager(data), data.issue.id,
        dict(values, next_action_at=(datetime.now(timezone.utc) + timedelta(days=1)).isoformat()))
    db.session.flush()
    assert row.actor_user_id == manager(data) and row.outcome == "NO_ANSWER"
    reception.finish_follow_up(data.org.id, manager(data), row.id)
    assert row.completed_by == manager(data)
    with pytest.raises(Conflict):
        reception.finish_follow_up(data.org.id, manager(data), row.id)


def test_municipal_lifecycle_reference_dates_and_original_issue(data):
    row = reception.referral(data.org.id, manager(data), data.issue.id, "Water")
    reception.transition_referral(data.org.id, manager(data), row.id, row.version, "SUBMITTED")
    with pytest.raises(BadRequest):
        reception.transition_referral(data.org.id, manager(data), row.id, row.version, "ACKNOWLEDGED")
    for state in ("ACKNOWLEDGED", "IN_PROGRESS", "RESPONSE_RECEIVED", "RESOLVED", "CLOSED"):
        reception.transition_referral(data.org.id, manager(data), row.id, row.version, state, "CITY-1", "Recorded response")
    db.session.flush()
    assert row.interaction_id == data.issue.id and data.issue.status != "RESOLVED"
    events = uip.UipReferralEvent.query.filter_by(referral_id=row.id).all()
    assert len(events) == 7 and all(e.actor_user_id == manager(data) for e in events)
    resolved = next(e for e in events if e.new_state == "RESOLVED")
    assert sla.utc(row.resolved_at) == sla.utc(resolved.occurred_at)


def test_communication_never_claims_delivery_and_rejects_foreign_links(data):
    values = dict(channel="EMAIL", direction="OUTBOUND", party_classification="MEMBER",
                  purpose="FOLLOW_UP", request_delivery="yes")
    row = reception.communication(data.org.id, manager(data), dict(values, interaction_id=data.issue.id))
    assert row.status == "DELIVERY_UNAVAILABLE"
    with pytest.raises(NotFound):
        reception.communication(data.org.id, manager(data), dict(values, interaction_id=data.foreign.id))
    with pytest.raises(BadRequest):
        reception.communication(data.org.id, manager(data), dict(values, status="SENT"))


def test_document_versions_private_download_and_cross_org(data, app, tmp_path):
    app.instance_path = str(tmp_path)
    values = dict(title="Policy", category="Policies", effective_date="2026-01-01", access_classification="PRIVATE")
    row, path = documents.upload(data.org.id, manager(data), FileStorage(io.BytesIO(b"%PDF-1.4\nTEST"), filename="../policy.pdf"), values)
    db.session.flush()
    assert path.is_file()
    with pytest.raises(NotFound):
        documents.download(data.org.id, data.users["resident"].id, row.id, 1)
    replacement = dict(values, expected_version=1, replacement_reason="Updated procedure")
    row, second = documents.upload(data.org.id, manager(data), FileStorage(io.BytesIO(b"%PDF-1.4\nNEW"), filename="policy.pdf"), replacement, row.id)
    db.session.flush()
    assert row.current_version == 2 and path.read_bytes() != second.read_bytes()
    assert uip.UipDocumentVersion.query.count() == 2
    with pytest.raises(NotFound):
        documents.download(data.other.id, data.outsider.id, row.id, 1)
    with pytest.raises(Conflict):
        documents.upload(data.org.id, manager(data), FileStorage(io.BytesIO(b"%PDF-1.4\nSTALE"), filename="policy.pdf"), replacement, row.id)


def eligible_member(data):
    membership = core.CoreOrganizationMember.query.filter_by(organization_id=data.org.id, user_id=data.users["resident"].id).one()
    member = make_member(data, eligibility_status="eligible", membership_id=str(membership.id))
    property_row = make_property(data)
    db.session.flush()
    db.session.add(uip.UipPropertyMember(organization_id=data.org.id, property_id=property_row.id, member_id=member.id,
        relationship="owner", valid_from=datetime.now(timezone.utc).date() - timedelta(days=2), is_verified=True))
    db.session.flush()
    return member


def concluded_meeting(data):
    member = eligible_member(data)
    governance.quorum_rule(data.org.id, manager(data), 50, 1, "owner")
    meeting = governance.meeting(data.org.id, manager(data), dict(title="AGM", meeting_type="AGM",
        scheduled_at=(datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(), location="Hall", agenda="Policies"))
    governance.start_meeting(data.org.id, manager(data), meeting.id)
    governance.attendance(data.org.id, manager(data), meeting.id, member.id, "PRESENT")
    governance.conclude(data.org.id, manager(data), meeting.id, "Policy discussed and voted on.")
    return meeting, member


def test_quorum_dated_eligibility_frozen_after_relationship_change(data):
    meeting, member = concluded_meeting(data)
    assert meeting.eligible_count == 1 and meeting.attendance_count == 1 and meeting.quorum_achieved
    member.eligibility_status = "ineligible"
    db.session.flush()
    assert governance.eligibility(data.org.id, datetime.now(timezone.utc).date(), "owner") == {}
    assert meeting.eligible_count == 1 and meeting.quorum_achieved
    with pytest.raises(Conflict):
        governance.attendance(data.org.id, manager(data), meeting.id, member.id, "ABSENT")


def test_login_roles_do_not_confer_eligibility(data):
    assert governance.eligibility(data.org.id, datetime.now(timezone.utc).date(), "owner") == {}


def test_survey_eligibility_duplicate_and_confidential_results(data, monkeypatch, client):
    member = eligible_member(data)
    now = datetime.now(timezone.utc)
    survey = governance.survey(data.org.id, manager(data), dict(title="Poll", purpose="Choose", relationship="owner",
        opens_at=(now - timedelta(minutes=1)).isoformat(), closes_at=(now + timedelta(minutes=10)).isoformat(), identifiable="no"),
        [dict(title="Approve?", type="YES_NO")])
    response = governance.respond(data.org.id, data.users["resident"].id, survey.id, member.id, {"1": "Yes"})
    assert response.eligibility_basis["property_links"]
    with pytest.raises(Conflict):
        governance.respond(data.org.id, data.users["resident"].id, survey.id, member.id, {"1": "No"})
    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return now + timedelta(minutes=11)
    monkeypatch.setattr(governance, "datetime", Later)
    governance.finalize(data.org.id, manager(data), survey.id)
    db.session.flush()
    assert survey.results["questions"]["1"] == {"Yes": 1, "No": 0}
    result = client.get(BASE + f"/surveys/{survey.id}")
    assert result.status_code == 200 and b"Member #" not in result.data


def test_decision_correction_preserves_source_and_history(data):
    meeting, member = concluded_meeting(data)
    values = dict(meeting_id=meeting.id, title="Policy", description="Adopt policy", votes_for=1, votes_against=0, abstentions=0)
    first = governance.decision(data.org.id, manager(data), values)
    second = governance.decision(data.org.id, manager(data), dict(values, supersedes_id=first.id, description="Corrected wording"))
    db.session.flush()
    assert first.description == "Adopt policy" and second.supersedes_id == first.id
    assert first.result_basis["votes"]["votes_for"] == 1
    assert uip.UipDecisionEvent.query.filter_by(decision_id=first.id).one().status == "SUPERSEDED"


@pytest.mark.parametrize("path", ["/sla", "/reception", "/documents", "/meetings", "/surveys", "/decisions", "/report.csv", "/providers"])
def test_provider_cannot_access_operational_pages(client, path):
    client.login("provider")
    assert client.get(BASE + path).status_code == 403


@pytest.mark.parametrize("path", ["/sla", "/reception", "/documents", "/meetings", "/surveys", "/decisions", "/providers"])
def test_manager_operational_pages_render(client, path):
    assert client.get(BASE + path).status_code == 200


@pytest.mark.parametrize("path", ["/sla", "/documents", "/meetings", "/surveys", "/decisions"])
def test_operational_posts_require_csrf(client, path):
    assert client.post(BASE + path, data={}).status_code == 400


def test_csv_scoped_and_formula_safe(client, data):
    data.issue.title = "=1+1"
    db.session.flush()
    response = client.get(BASE + "/report.csv")
    assert response.status_code == 200 and b"'=1+1" in response.data
    assert b"OTHER-TEST" not in response.data


def test_metrics_no_invented_samples_or_cross_org(data):
    result = metrics(data.org.id, manager(data))
    assert result["Open issues"] == 1
    assert result["Recorded acknowledgement mean minutes (sample count)"] == "No recorded sample"
    assert result["Open surveys"] == 0


def test_governance_foreign_sources_and_tasks_are_rejected(data):
    from app.uip.services import operations
    foreign = governance.meeting(data.other.id, data.outsider.id, dict(title="Other", meeting_type="AGM",
        scheduled_at=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), location="Other hall", agenda="Other"))
    with pytest.raises(NotFound):
        governance.start_meeting(data.org.id, manager(data), foreign.id)
    meeting, member = concluded_meeting(data)
    task = operations.add_task(data.other.id, data.outsider.id, data.foreign.id, "Other task")
    with pytest.raises(NotFound):
        governance.decision(data.org.id, manager(data), dict(meeting_id=meeting.id, title="Scoped", description="Scoped",
            votes_for=1, votes_against=0, abstentions=0, linked_task_id=task.id))


def test_quorum_configuration_is_required(data):
    meeting = governance.meeting(data.org.id, manager(data), dict(title="No rule", meeting_type="AGM",
        scheduled_at=(datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), location="Hall", agenda="Agenda"))
    with pytest.raises(Conflict):
        governance.start_meeting(data.org.id, manager(data), meeting.id)
    assert meeting.eligibility_basis is None


def test_expired_property_relationship_is_not_eligible(data):
    member = eligible_member(data)
    ownership = uip.UipPropertyMember.query.filter_by(member_id=member.id).one()
    ownership.valid_to = datetime.now(timezone.utc).date() - timedelta(days=1)
    db.session.flush()
    assert governance.eligibility(data.org.id, datetime.now(timezone.utc).date(), "owner") == {}


def test_operational_audit_and_mutation_rollback_together(data):
    before = uip.UipAuditEvent.query.count()
    with pytest.raises(RuntimeError):
        with db.session.begin_nested():
            reception.follow_up(data.org.id, manager(data), data.issue.id,
                dict(method="INTERNAL", outcome="INFORMATION_RECEIVED", next_action="NONE", note="Private note"))
            db.session.flush()
            assert uip.UipAuditEvent.query.count() == before + 1
            raise RuntimeError("Simulated rollback")
    assert uip.UipFollowUp.query.count() == 0
    assert uip.UipAuditEvent.query.count() == before


def test_database_protects_concluded_attendance_and_decision_content(data):
    import sqlalchemy as sa
    meeting, member = concluded_meeting(data)
    db.session.flush()
    with pytest.raises(sa.exc.DBAPIError):
        with db.session.begin_nested():
            db.session.execute(sa.text("UPDATE uip_meeting_participant SET status='ABSENT' WHERE meeting_id=:id"), {"id": meeting.id})
    decision = governance.decision(data.org.id, manager(data), dict(meeting_id=meeting.id, title="Immutable",
        description="Original", votes_for=1, votes_against=0, abstentions=0))
    db.session.flush()
    with pytest.raises(sa.exc.DBAPIError):
        with db.session.begin_nested():
            db.session.execute(sa.text("UPDATE uip_resolution SET description='Changed' WHERE id=:id"), {"id": decision.id})
    assert decision.description == "Original"
