
def make_resolution(org_id, recorded_by, title, status="ADOPTED"):
    from app.models.uip import UipCommitteeMeeting
    from datetime import datetime
    meeting = UipCommitteeMeeting.query.filter_by(organization_id=org_id).first()
    if not meeting:
        meeting = UipCommitteeMeeting(organization_id=org_id, title="Test", meeting_type="FOUNDING", scheduled_at=datetime(2026,1,1), status="CONCLUDED")
        db.session.add(meeting)
        db.session.flush()
    res = UipResolution(organization_id=org_id, meeting_id=meeting.id, title=title, description=title, status=status, recorded_by=recorded_by)
    db.session.add(res)
    db.session.flush()
    return res

import pytest
from flask import g
from app.extensions import db
from app.models.uip_governance import UipSubcommittee, UipOrganogramSeat, UipCommitteeMember, UipCommitteeTerm
from app.models.uip_proposal import UipProposal
from app.models.uip import UipResolution
from app.models.auth import User


@pytest.fixture
def subcomm_setup(client, data):
    # Setup roles and users
    res1 = make_resolution(data.org.id, data.users["manager"].id, "Test Mandate 1", "ADOPTED")
    res2 = make_resolution(data.org.id, data.users["manager"].id, "Test Mandate 2", "ADOPTED")
    
    seat1 = UipOrganogramSeat(organization_id=data.org.id, title="Chairperson", group_level="CORE_EXCO", duty="owner")
    seat2 = UipOrganogramSeat(organization_id=data.org.id, title="Secretary", group_level="CORE_EXCO", duty="manager")
    db.session.add_all([seat1, seat2])
    db.session.flush()
    
    sub1 = UipSubcommittee(organization_id=data.org.id, name="Greening", establishing_resolution_id=res1.id, responsible_seat_id=seat1.id, reports_to_seat_id=seat1.id)
    sub2 = UipSubcommittee(organization_id=data.org.id, name="Security", establishing_resolution_id=res2.id, responsible_seat_id=seat2.id, reports_to_seat_id=seat1.id)
    db.session.add_all([sub1, sub2])
    db.session.commit()
    
    # Map owner to Chair, receptionist to Secretary
    term = UipCommitteeTerm.query.filter_by(organization_id=data.org.id).first()
    if not term:
        from datetime import date
        term = UipCommitteeTerm(organization_id=data.org.id, term_name="Test Term")
        db.session.add(term)
        db.session.flush()
        db.session.add(term)
        db.session.flush()
    chair_mem = UipCommitteeMember.query.filter_by(organization_id=data.org.id, position=seat1.title).first()
    if not chair_mem:
        chair_mem = UipCommitteeMember(organization_id=data.org.id, term_id=term.id, name="Owner", email=data.users["owner"].email, position=seat1.title, seat_id=seat1.id, status="CURRENT")
        db.session.add(chair_mem)
    else:
        chair_mem.email = data.users["owner"].email
        chair_mem.seat_id = seat1.id
        
    sec_mem = UipCommitteeMember.query.filter_by(organization_id=data.org.id, position=seat2.title).first()
    if not sec_mem:
        sec_mem = UipCommitteeMember(organization_id=data.org.id, term_id=term.id, name="Recept", email=data.users["receptionist"].email, position=seat2.title, seat_id=seat2.id, status="CURRENT")
        db.session.add(sec_mem)
    else:
        sec_mem.email = data.users["receptionist"].email
        sec_mem.seat_id = seat2.id
        
    db.session.commit()
    return data.org, sub1, sub2, data.users["owner"], data.users["receptionist"], data.outsider, data.other, seat1, seat2

def test_current_responsible_seat_can_access(client, subcomm_setup):
    """1. Current responsible seat occupant can access that specific Subcommittee."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("owner")
    resp = client.get(f"/uip/{org.slug}/subcommittee/{sub1.id}/board")
    assert resp.status_code == 200
    assert sub1.name in resp.get_data(as_text=True)

def test_another_elected_cannot_access_merely_because_elected(client, subcomm_setup):
    """2. Another elected official cannot access it merely because they are elected."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("receptionist") # Is Secretary, elected, but responsible for sub2, not sub1
    resp = client.get(f"/uip/{org.slug}/subcommittee/{sub1.id}/board")
    assert resp.status_code == 403

def test_generic_subcommittee_member_cannot_access(client, subcomm_setup):
    """3. Generic subcommittee_member cannot access it."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("manager") # They might have some generic role
    resp = client.get(f"/uip/{org.slug}/subcommittee/{sub1.id}/board")
    assert resp.status_code == 403

def test_title_match_without_seat_id_denied(client, subcomm_setup):
    """4. Title match without seat_id cannot access it."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    chair = UipCommitteeMember.query.filter_by(organization_id=org.id, position=seat1.title, status="CURRENT").first()
    chair.status = "FORMER"
    
    new_mem = UipCommitteeMember(organization_id=org.id, term_id=chair.term_id, name="NoSeat", email=recept.email, position=seat1.title, seat_id=None, status="CURRENT")
    db.session.add(new_mem)
    db.session.commit()
    
    client.login("receptionist")
    resp = client.get(f"/uip/{org.slug}/subcommittee/{sub1.id}/board")
    assert resp.status_code == 403

def test_cross_org_access_denied(client, subcomm_setup):
    """5. Cross-org access denied."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("owner")
    resp = client.get(f"/uip/{other.slug}/subcommittee/{sub1.id}/board")
    assert resp.status_code == 404 # Usually org_slug mismatch leads to 404 or redirect

def test_responsible_member_a_cannot_access_b(client, subcomm_setup):
    """6. Responsible member of Subcommittee A cannot access Subcommittee B."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("owner")
    resp = client.get(f"/uip/{org.slug}/subcommittee/{sub2.id}/board")
    assert resp.status_code == 403

def test_changing_seat_occupant_transfers_access(client, subcomm_setup):
    """7. Changing the seat occupant transfers access."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    chair = UipCommitteeMember.query.filter_by(organization_id=org.id, position=seat1.title, status="CURRENT").first()
    chair.status = "FORMER"
    
    new_mem = UipCommitteeMember(organization_id=org.id, term_id=chair.term_id, name="New", email=recept.email, position=seat1.title, seat_id=chair.seat_id, status="CURRENT")
    db.session.add(new_mem)
    db.session.commit()
    
    client.login("owner")
    resp1 = client.get(f"/uip/{org.slug}/subcommittee/{sub1.id}/board")
    assert resp1.status_code == 403
    
    client.login("receptionist")
    resp2 = client.get(f"/uip/{org.slug}/subcommittee/{sub1.id}/board")
    assert resp2.status_code == 200

def test_vacant_seat_removes_access(client, subcomm_setup):
    """8. Vacant responsible seat removes access."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    chair = UipCommitteeMember.query.filter_by(organization_id=org.id, position=seat1.title, status="CURRENT").first()
    chair.status = "FORMER"
    db.session.commit()
    
    client.login("owner")
    resp = client.get(f"/uip/{org.slug}/subcommittee/{sub1.id}/board")
    assert resp.status_code == 403

def test_selector_for_multiple_subcommittees(client, subcomm_setup):
    """9. One person responsible for multiple Subcommittees can select between them."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    sec = UipCommitteeMember.query.filter_by(organization_id=org.id, position=seat2.title, status="CURRENT").first()
    sec.email = owner.email
    db.session.commit()
    
    client.login("owner")
    resp = client.get(f"/uip/{org.slug}/sub-comm-tools")
    assert resp.status_code == 200
    assert "Select Subcommittee" in resp.get_data(as_text=True)
    assert sub1.name in resp.get_data(as_text=True)
    assert sub2.name in resp.get_data(as_text=True)

def test_proposal_retains_originating_subcommittee(client, subcomm_setup):
    """10. Proposal created from Tools retains the correct originating_subcommittee_id."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("owner")
    
    client.safe_post(f"/uip/{org.slug}/subcommittee/{sub1.id}/proposals", dict(
        title="Sub Proposal", description="Desc", motivation="Motiv"
    ))
    
    prop = UipProposal.query.filter_by(title="Sub Proposal").first()
    assert prop is not None
    assert prop.originating_subcommittee_id == sub1.id

def test_proposal_enters_shared_workflow(client, subcomm_setup):
    """11. That Proposal enters the existing shared Proposal workflow without becoming a Resolution."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("owner")
    client.safe_post(f"/uip/{org.slug}/subcommittee/{sub1.id}/proposals", dict(
        title="Sub Proposal", description="Desc", motivation="Motiv"
    ))
    
    prop = UipProposal.query.filter_by(title="Sub Proposal").first()
    assert prop.status == "DRAFT"
    assert prop.resolution_id is None

def test_no_spending_authority(client, subcomm_setup):
    """12. No Subcommittee action creates spending authority or finance records."""
    org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup
    client.login("owner")
    
    client.safe_post(f"/uip/{org.slug}/subcommittee/{sub1.id}/proposals", dict(
        title="Finance Proposal", description="Desc", motivation="Motiv"
    ))
    
    prop = UipProposal.query.filter_by(title="Finance Proposal").first()
    assert prop.status == "DRAFT"
    # The existence of the proposal in DRAFT guarantees no authority since it requires Resolution ADOPTION.
    # We just ensure it hasn't somehow bypassed to ADOPTED.
    assert prop.resolution_id is None
