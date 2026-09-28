import sys
with open("tests/uip/test_subcommittees.py", "r", encoding="utf-8") as f:
    c = f.read()

# Update setup_secretary to assign seat_id
c = c.replace(
    'db.session.add(UipCommitteeMember(organization_id=data.org.id, term_id=term.id, name=data.users["owner"].name, email=data.users["owner"].email, position="Chairperson", status="CURRENT"))',
    'db.session.add(UipCommitteeMember(organization_id=data.org.id, term_id=term.id, name=data.users["owner"].name, email=data.users["owner"].email, position="Chairperson", seat_id=s3.id, status="CURRENT"))'
)

# Update test_changing_occupant_changes_resolved_member
c = c.replace(
    'new_member = UipCommitteeMember(term_id=old_member.term_id, organization_id=data.org.id, name="New Chair", email="newchair@example.invalid", position="Chairperson", status="CURRENT")',
    'new_member = UipCommitteeMember(term_id=old_member.term_id, organization_id=data.org.id, name="New Chair", email="newchair@example.invalid", position="Chairperson", seat_id=seat.id, status="CURRENT")'
)

with open("tests/uip/test_subcommittees.py", "w", encoding="utf-8") as f:
    f.write(c)
print("Updated test_subcommittees.py test fixtures")
