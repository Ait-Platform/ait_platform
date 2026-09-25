"""Proposal HTTP boundaries on disposable PostgreSQL; no application factory."""
from datetime import datetime
from decimal import Decimal
import importlib.util
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from bootstrap import db, core, uip, ROOT
from app.models.uip_governance import UipCommitteeMember, UipCommitteeTerm
from app.models.uip_proposal import UipProposal, UipProposalDocument
from app.models.uip_finance import UipFinanceTransaction, UipFinanceCommitment, UipFinanceBudgetLine

BASE = "/uip/manor-gardens/proposals"
OFFICIALS = {"resident": "Secretary", "owner": "Chairperson", "provider": "Vice-Chairperson", "receptionist": "Treasurer"}
VALUES = dict(title="Shared park proposal", description="Restore the park", motivation="Safe public space", proposed_budget="1200.25")




@pytest.fixture
def officials(data):
    term = UipCommitteeTerm(organization_id=data.org.id, term_name="Verified elected term")
    db.session.add(term); db.session.flush()
    appointments = {}
    for role, position in OFFICIALS.items():
        user = data.users[role]
        row = UipCommitteeMember(organization_id=data.org.id, term_id=term.id,
            name=user.name, email=user.email, position=position, status="CURRENT")
        db.session.add(row); appointments[role] = row
    meeting = uip.UipCommitteeMeeting(organization_id=data.org.id, title="Recorded meeting",
        meeting_type="FOUNDING", scheduled_at=datetime(2026, 9, 1), status="CONCLUDED")
    db.session.add(meeting); db.session.commit()
    return appointments, meeting


def create(client, role="resident", **values):
    client.login(role)
    response = client.safe_post(BASE + "/new", {**VALUES, **values})
    assert response.status_code == 302
    return UipProposal.query.order_by(UipProposal.id.desc()).first()


@pytest.mark.parametrize("role", OFFICIALS)
def test_official_create_submit(client, data, officials, role):
    client.login(role)
    assert client.get(BASE + "/new").status_code == 200
    row = create(client, role, originating_capacity="manager", originator_id=data.outsider.id)
    assert row.originator_id == (data.users[role].id if role != "municipal_officer" else data.users["manager"].id)
    assert row.originating_capacity == OFFICIALS[role]
    assert row.originating_subcommittee_id is None
    assert row.proposed_budget == Decimal("1200.25")
    assert row.status == "DRAFT" and row.submitted_at is None
    assert client.safe_post(BASE + f"/{row.id}/submit").status_code == 302
    assert row.status == "SUBMITTED" and row.submitted_at is not None
    assert client.get(BASE + f"/{row.id}").status_code == 200
    assert UipFinanceTransaction.query.count() == UipFinanceCommitment.query.count() == UipFinanceBudgetLine.query.count() == 0


@pytest.mark.parametrize("role", ["manager", "committee_member"])
def test_core_roles_and_pending_claim_do_not_grant_authority(client, data, role):
    db.session.add(core.CoreInteraction(organization_id=data.org.id, creator_id=data.users[role].id,
        reference="PENDING-PROP", title="Secretary claim", interaction_type="role_claim", status="NEW"))
    db.session.commit(); client.login(role)
    assert client.get(BASE).status_code == 403
    assert client.safe_post(BASE + "/new", VALUES).status_code == 403
    assert UipProposal.query.count() == 0


def test_org_wide_subcommittee_role_denied(client, data):
    user = data.users["committee_member"]
    role = core.CoreRole(organization_id=data.org.id, name="Subcommittee", slug="subcommittee_member")
    db.session.add(role); db.session.flush()
    db.session.add(core.CoreRoleAssignment(organization_id=data.org.id, user_id=user.id, role_id=role.id))
    db.session.commit(); client.login("committee_member")
    assert client.safe_post(BASE + "/123/convert", {"meeting_id":"1"}).status_code == 403
    assert UipProposal.query.count() == 0


def test_one_shared_record_draft_private_then_visible_to_all(client, data, officials):
    row = create(client)
    for role in ("owner", "provider", "receptionist"):
        client.login(role)
        assert row.title.encode() not in client.get(BASE).data
        assert client.get(BASE + f"/{row.id}").status_code == 404
        assert client.safe_post(BASE + f"/{row.id}/edit", VALUES).status_code == 404
    client.login("resident")
    assert client.safe_post(BASE + f"/{row.id}/submit").status_code == 302
    for role in OFFICIALS:
        client.login(role)
        assert row.reference.encode() in client.get(BASE).data
        response = client.get(BASE + f"/{row.id}")
        assert response.status_code == 200 and row.title.encode() in response.data
    assert UipProposal.query.count() == 1
    assert UipResolution_count() == 0


def UipResolution_count():
    return uip.UipResolution.query.count()


@pytest.mark.parametrize("role", OFFICIALS)
def test_conversion_exactly_once_and_only_draft(client, data, officials, role):
    row = create(client)
    client.safe_post(BASE + f"/{row.id}/submit")
    client.login(role)
    values = {"meeting_id":str(officials[1].id)}
    first = client.safe_post(BASE + f"/{row.id}/convert", values)
    second = client.safe_post(BASE + f"/{row.id}/convert", values)
    assert first.status_code == second.status_code == 302
    assert first.location == second.location
    resolution = uip.UipResolution.query.one()
    assert row.resolution_id == resolution.id and row.status == "CONVERTED"
    assert row.converted_by == (data.users[role].id if role != "municipal_officer" else data.users["manager"].id) and row.converted_at is not None
    assert resolution.status == "DRAFT" and resolution.decision_date is None
    assert resolution.result_basis["proposal_id"] == row.id
    assert resolution.recorded_by == (data.users[role].id if role != "municipal_officer" else data.users["manager"].id)
    assert resolution.linked_task_id is None
    assert uip.UipResolutionVote.query.count() == 0
    assert UipFinanceTransaction.query.count() == UipFinanceCommitment.query.count() == UipFinanceBudgetLine.query.count() == 0
    assert client.get(first.location).status_code == 200
    assert client.safe_post(BASE + f"/{row.id}/edit", VALUES).status_code == 409


def test_cross_org_access_and_meeting_denied(client, data, officials):
    row = create(client)
    client.safe_post(BASE + f"/{row.id}/submit")
    foreign = uip.UipCommitteeMeeting(organization_id=data.other.id, title="Other meeting", scheduled_at=datetime(2026,9,1))
    term = UipCommitteeTerm(organization_id=data.other.id, term_name="Other elected term")
    db.session.add_all([foreign, term]); db.session.flush()
    db.session.add(UipCommitteeMember(organization_id=data.other.id, term_id=term.id, name="Other Secretary", email=data.outsider.email, position="Secretary", status="CURRENT"))
    db.session.commit()
    assert client.safe_post(BASE + f"/{row.id}/convert", {"meeting_id":foreign.id}).status_code == 404
    client.login("outsider")
    assert client.get(BASE + f"/{row.id}").status_code == 403
    other = "/uip/other/proposals"
    assert row.title.encode() not in client.get(other).data
    assert client.get(other + f"/{row.id}").status_code == 404
    assert client.safe_post(other + f"/{row.id}/convert", {"meeting_id":foreign.id}).status_code == 404
    assert UipResolution_count() == 0


@pytest.mark.parametrize("state", ["FORMER", "inactive", "foreign"])
def test_current_active_same_org_appointment_required(client, data, officials, state):
    if state == "inactive": data.users["resident"].is_active = 0
    elif state == "foreign": officials[0]["resident"].organization_id = data.other.id
    else: officials[0]["resident"].status = state
    db.session.commit(); client.login("resident")
    assert client.safe_post(BASE + "/new", VALUES).status_code == (401 if state == "inactive" else 403)
    assert UipProposal.query.count() == 0


def test_draft_edit_and_validation(client, data, officials):
    row = create(client, proposed_budget="")
    assert row.proposed_budget is None
    assert client.safe_post(BASE + f"/{row.id}/convert", {"meeting_id":officials[1].id}).status_code == 409
    assert client.safe_post(BASE + f"/{row.id}/edit", {**VALUES,"title":"Updated"}).status_code == 302
    assert row.title == "Updated"
    for amount in ("NaN", "1.001", "-1", "100000000000000.00"):
        assert client.safe_post(BASE + "/new", {**VALUES,"proposed_budget":amount}).status_code == 400
        assert UipProposal.query.count() == 1


def test_documents_reuse_and_existing_access(client, data, officials):
    client.login("resident")
    doc = uip.UipDocument(organization_id=data.org.id, uploader_id=data.users["resident"].id,
        title="Existing evidence", filename="evidence.pdf", access_classification="MEMBERS")
    private = uip.UipDocument(organization_id=data.org.id, uploader_id=data.users["manager"].id,
        title="Private evidence", filename="private.pdf", access_classification="PRIVATE")
    foreign = uip.UipDocument(organization_id=data.other.id, uploader_id=data.outsider.id,
        filename="foreign.pdf", access_classification="MEMBERS")
    db.session.add_all([doc,private,foreign]); db.session.commit()
    row = create(client, document_id=str(doc.id))
    assert UipProposalDocument.query.one().document_id == doc.id
    assert client.get(BASE + f"/{row.id}").status_code == 200
    for denied in (private, foreign):
        assert client.safe_post(BASE + "/new", {**VALUES,"document_id":str(denied.id)}).status_code == 404
    assert UipProposal.query.count() == 1 and uip.UipDocument.query.count() == 3


def test_navigation_for_four_officials(client, data, officials):
    for role in OFFICIALS:
        client.login(role)
        page = client.get(BASE)
        assert page.status_code == 200
        assert b"New Proposal" in page.data and b"Resolution Register" in page.data
        assert b"Formal Resolution drafting" in page.data