"""Focused tests for Subcommittee structural layer."""
import pytest
from app.extensions import db
from app.models.uip import UipResolution
from app.models.uip_governance import UipOrganogramSeat, UipCommitteeMember, UipSubcommittee, UipCommitteeTerm
from app.program_uip.services.subcommittees import resolve_responsible_member

BASE = "/uip/manor-gardens"

@pytest.fixture(autouse=True)
def setup_secretary(data):
    # Make "manager" the Secretary
    term = UipCommitteeTerm(organization_id=data.org.id, term_name="Test Term")
    db.session.add(term)
    db.session.flush()
    sec = UipCommitteeMember(organization_id=data.org.id, term_id=term.id, name=data.users["manager"].name, email=data.users["manager"].email, position="Secretary", status="CURRENT")
    db.session.add(sec)
    
    # Also add standard seats to the org
    s1 = UipOrganogramSeat(organization_id=data.org.id, title="Greening & Environment Member", group_level="SECOND_GROUP", duty="committee_member")
    s2 = UipOrganogramSeat(organization_id=data.org.id, title="Vice-Chairperson", group_level="CORE_EXCO", duty="manager")
    s3 = UipOrganogramSeat(organization_id=data.org.id, title="Chairperson", group_level="CORE_EXCO", duty="owner")
    s4 = UipOrganogramSeat(organization_id=data.org.id, title="Treasurer", group_level="CORE_EXCO", duty="treasurer")
    db.session.add_all([s1, s2, s3, s4])
    
    # Populate members for seats
    db.session.add(UipCommitteeMember(organization_id=data.org.id, term_id=term.id, name=data.users["owner"].name, email=data.users["owner"].email, position="Chairperson", seat_id=s3.id, status="CURRENT"))
    db.session.commit()

def make_resolution(org_id, recorded_by, title, status="ADOPTED"):
    from app.models.uip import UipCommitteeMeeting
    from datetime import datetime
    meeting = UipCommitteeMeeting.query.filter_by(organization_id=org_id).first()
    if not meeting:
        meeting = UipCommitteeMeeting(organization_id=org_id, title="Test", meeting_type="FOUNDING", scheduled_at=datetime(2026,1,1), status="CONCLUDED")
        db.session.add(meeting)
        db.session.flush()
    res = UipResolution(organization_id=org_id, recorded_by=recorded_by, title=title, 
                        status=status, description="Content", meeting_id=meeting.id)
    db.session.add(res)
    db.session.flush()
    return res

def test_subcommittee_creation_from_adopted(client, data):
    """1. Secretary can create a specific Subcommittee from a same-org ADOPTED Resolution."""
    client.login("manager") # Manager is the secretary in our setup
    res = make_resolution(data.org.id, data.users["manager"].id, "Green Mandate", "ADOPTED")
    db.session.commit()
    
    seat1 = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Greening & Environment Member").first()
    seat2 = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Vice-Chairperson").first()
    
    resp = client.safe_post(BASE + "/secretary/organogram", dict(
        action="add_subcommittee",
        name="Greening Subcommittee",
        resolution_id=res.id,
        responsible_seat_id=seat1.id,
        reports_to_seat_id=seat2.id
    ))
    assert resp.status_code == 302
    sub = UipSubcommittee.query.filter_by(name="Greening Subcommittee").one()
    assert sub.establishing_resolution_id == res.id
    assert sub.responsible_seat_id == seat1.id

def test_subcommittee_draft_tabled_rejected(client, data):
    """2. DRAFT/PROPOSED/TABLED Resolution cannot establish it."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Draft Mandate", "DRAFT")
    seat1 = UipOrganogramSeat.query.filter_by(organization_id=data.org.id).first()
    db.session.commit()
    
    resp = client.safe_post(BASE + "/secretary/organogram", dict(
        action="add_subcommittee",
        name="Invalid Subcommittee",
        resolution_id=res.id,
        responsible_seat_id=seat1.id,
        reports_to_seat_id=seat1.id
    ))
    resp = client.get(BASE + "/secretary/organogram")
    assert "A Subcommittee must be established by a same-organisation ADOPTED resolution" in resp.get_data(as_text=True)
    assert UipSubcommittee.query.filter_by(name="Invalid Subcommittee").count() == 0

def test_subcommittee_cross_org_resolution_rejected(client, data):
    """3. Cross-organisation Resolution cannot establish it."""
    client.login("manager")
    res = make_resolution(data.other.id, data.outsider.id, "Other Mandate", "ADOPTED")
    seat1 = UipOrganogramSeat.query.filter_by(organization_id=data.org.id).first()
    db.session.commit()
    
    resp = client.safe_post(BASE + "/secretary/organogram", dict(
        action="add_subcommittee",
        name="Cross Org Subcommittee",
        resolution_id=res.id,
        responsible_seat_id=seat1.id,
        reports_to_seat_id=seat1.id
    ))
    resp = client.get(BASE + "/secretary/organogram")
    assert "same-organisation ADOPTED resolution" in resp.get_data(as_text=True)

def test_subcommittee_cross_org_seats_rejected(client, data):
    """4. Responsible seat and reports-to seat must belong to the same organisation."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Valid Mandate", "ADOPTED")
    seat_other = UipOrganogramSeat(organization_id=data.other.id, title="Other Seat", group_level="CORE_EXCO", duty="owner")
    db.session.add(seat_other)
    db.session.commit()
    
    resp = client.safe_post(BASE + "/secretary/organogram", dict(
        action="add_subcommittee",
        name="Cross Seat Subcommittee",
        resolution_id=res.id,
        responsible_seat_id=seat_other.id,
        reports_to_seat_id=seat_other.id
    ))
    resp = client.get(BASE + "/secretary/organogram")
    assert "seats must be valid and belong to this organisation" in resp.get_data(as_text=True)

def test_subcommittee_generic_role_denied(client, data):
    """5. Generic subcommittee_member alone cannot create/edit the structure."""
    client.login("committee_member") 
    res = make_resolution(data.org.id, data.users["manager"].id, "Mandate", "ADOPTED")
    seat1 = UipOrganogramSeat.query.filter_by(organization_id=data.org.id).first()
    db.session.commit()
    
    resp = client.safe_post(BASE + "/secretary/organogram", dict(
        action="add_subcommittee",
        name="Hacked Subcommittee",
        resolution_id=res.id,
        responsible_seat_id=seat1.id,
        reports_to_seat_id=seat1.id
    ))
    assert resp.status_code == 403

def test_responsible_seat_occupant_resolved(client, data):
    """6. Current occupant of responsible_seat_id resolves as the responsible elected member."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Mandate", "ADOPTED")
    seat = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Chairperson").first()
    sub = UipSubcommittee(organization_id=data.org.id, name="Test Sub", establishing_resolution_id=res.id, responsible_seat_id=seat.id, reports_to_seat_id=seat.id)
    db.session.add(sub)
    db.session.commit()
    
    member = resolve_responsible_member(sub)
    assert member is not None
    assert member.email == data.users["owner"].email

def test_changing_occupant_changes_resolved_member(client, data):
    """7. Changing the current occupant changes the resolved responsible member without rewriting the Subcommittee."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Mandate", "ADOPTED")
    seat = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Chairperson").first()
    sub = UipSubcommittee(organization_id=data.org.id, name="Dynamic Sub", establishing_resolution_id=res.id, responsible_seat_id=seat.id, reports_to_seat_id=seat.id)
    db.session.add(sub)
    
    old_member = UipCommitteeMember.query.filter_by(organization_id=data.org.id, position="Chairperson", status="CURRENT").first()
    old_member.status = "FORMER"
    
    new_member = UipCommitteeMember(term_id=old_member.term_id, organization_id=data.org.id, name="New Chair", email="newchair@example.invalid", position="Chairperson", seat_id=seat.id, status="CURRENT")
    db.session.add(new_member)
    db.session.commit()
    
    member = resolve_responsible_member(sub)
    assert member.email == "newchair@example.invalid"

def test_vacant_seat_yields_none(client, data):
    """8. Vacant responsible seat yields no responsible person/authority."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Mandate", "ADOPTED")
    seat = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Chairperson").first()
    sub = UipSubcommittee(organization_id=data.org.id, name="Vacant Sub", establishing_resolution_id=res.id, responsible_seat_id=seat.id, reports_to_seat_id=seat.id)
    db.session.add(sub)
    
    old_member = UipCommitteeMember.query.filter_by(organization_id=data.org.id, position="Chairperson", status="CURRENT").first()
    old_member.status = "FORMER"
    db.session.commit()
    
    assert resolve_responsible_member(sub) is None

def test_reporting_seat_data_driven(client, data):
    """9. Reporting seat is data-driven and not hardcoded."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Mandate", "ADOPTED")
    seat_treasurer = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Treasurer").first()
    sub = UipSubcommittee(organization_id=data.org.id, name="Treasury Sub", establishing_resolution_id=res.id, responsible_seat_id=seat_treasurer.id, reports_to_seat_id=seat_treasurer.id)
    db.session.add(sub)
    db.session.commit()
    assert sub.reports_to_seat_id == seat_treasurer.id

def test_organization_isolation(client, data):
    """10. Organisation isolation is enforced."""
    client.login("manager")
    res_other = make_resolution(data.other.id, data.outsider.id, "Other", "ADOPTED")
    seat_other = UipOrganogramSeat(organization_id=data.other.id, title="Other", group_level="CORE_EXCO", duty="owner")
    db.session.add(seat_other)
    db.session.flush()
    sub_other = UipSubcommittee(organization_id=data.other.id, name="Other Sub", establishing_resolution_id=res_other.id, responsible_seat_id=seat_other.id, reports_to_seat_id=seat_other.id)
    db.session.add(sub_other)
    db.session.commit()
    
    resp = client.get(BASE + "/secretary/organogram")
    assert "Other Sub" not in resp.get_data(as_text=True)

def test_duplicate_active_subcommittee_prevented(client, data):
    """11. Duplicate active representation of the same Subcommittee is prevented."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Green Mandate", "ADOPTED")
    db.session.commit()
    seat = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Chairperson").first()
    
    resp1 = client.safe_post(BASE + "/secretary/organogram", dict(
        action="add_subcommittee", name="Duplicate Sub", resolution_id=res.id, responsible_seat_id=seat.id, reports_to_seat_id=seat.id
    ))
    assert resp1.status_code == 302
    
    resp2 = client.safe_post(BASE + "/secretary/organogram", dict(
        action="add_subcommittee", name="Duplicate Sub", resolution_id=res.id, responsible_seat_id=seat.id, reports_to_seat_id=seat.id
    ))
    assert "An active subcommittee named &#39;Duplicate Sub&#39; already exists" in client.get(BASE + '/secretary/organogram').get_data(as_text=True)
    assert UipSubcommittee.query.filter_by(name="Duplicate Sub").count() == 1





def test_title_without_seat_id_denied(client, data):
    """Matching position/title without the correct seat_id does NOT confer Subcommittee authority."""
    client.login("manager")
    res = make_resolution(data.org.id, data.users["manager"].id, "Mandate", "ADOPTED")
    seat = UipOrganogramSeat.query.filter_by(organization_id=data.org.id, title="Chairperson").first()
    sub = UipSubcommittee(organization_id=data.org.id, name="Test Sub", establishing_resolution_id=res.id, responsible_seat_id=seat.id, reports_to_seat_id=seat.id)
    db.session.add(sub)
    
    # Existing occupant leaves
    old_member = UipCommitteeMember.query.filter_by(organization_id=data.org.id, position="Chairperson", status="CURRENT").first()
    old_member.status = "FORMER"
    
    # New member has title, but no seat_id
    new_member = UipCommitteeMember(term_id=old_member.term_id, organization_id=data.org.id, name="Fake Chair", email="fake@example.invalid", position="Chairperson", status="CURRENT")
    db.session.add(new_member)
    db.session.commit()
    
    assert resolve_responsible_member(sub) is None
