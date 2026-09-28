import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'seat1 = UipOrganogramSeat(organization_id=data.org.id, title="Chairperson")\n    seat2 = UipOrganogramSeat(organization_id=data.org.id, title="Secretary")',
    'seat1 = UipOrganogramSeat(organization_id=data.org.id, title="Chairperson", group_level="CORE_EXCO", duty="owner")\n    seat2 = UipOrganogramSeat(organization_id=data.org.id, title="Secretary", group_level="CORE_EXCO", duty="manager")'
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
