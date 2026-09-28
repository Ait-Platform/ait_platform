import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

# Fix subcomm_setup return
c = c.replace(
    'return data.org, sub1, sub2, data.users["owner"], data.users["receptionist"], data.outsider, data.other',
    'return data.org, sub1, sub2, data.users["owner"], data.users["receptionist"], data.outsider, data.other, seat1, seat2'
)

c = c.replace(
    'org, sub1, sub2, owner, recept, outsider, other = subcomm_setup',
    'org, sub1, sub2, owner, recept, outsider, other, seat1, seat2 = subcomm_setup'
)

# Fix follow_redirects
c = c.replace(
    '), follow_redirects=True)',
    ')'
)

# test_current_responsible_seat_can_access 404? 
# Maybe sub_id is wrong, or slug is wrong. org.slug might be missing?
# Wait, data.org is just org. The route is f"/uip/{org.slug}/subcommittee/{sub1.id}/board".
# I'll let it run and if it fails I'll check.

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
