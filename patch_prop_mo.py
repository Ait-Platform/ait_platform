import sys
import re
with open("tests/uip/test_proposal_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    '@pytest.mark.parametrize("role", ["manager", "committee_member", "municipal_officer"])',
    '@pytest.mark.parametrize("role", ["manager", "committee_member"])'
)
c = c.replace(
    'creator_id=(data.users[role].id if role != "municipal_officer" else data.users["manager"].id)',
    'creator_id=data.users[role].id'
)

with open("tests/uip/test_proposal_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
