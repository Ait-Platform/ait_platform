import sys
with open("tests/uip/test_subcommittees.py", "r", encoding="utf-8") as f:
    c = f.read()

new_test = """
def test_title_without_seat_id_denied(client, data):
    \"\"\"Matching position/title without the correct seat_id does NOT confer Subcommittee authority.\"\"\"
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
"""

if "test_title_without_seat_id_denied" not in c:
    c += new_test

with open("tests/uip/test_subcommittees.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Updated test_subcommittees.py")
