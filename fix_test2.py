import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    "res = UipResolution(organization_id=org_id, meeting_id=meeting.id, description=title, status=status, recorded_by=recorded_by)",
    "res = UipResolution(organization_id=org_id, meeting_id=meeting.id, title=title, description=title, status=status, recorded_by=recorded_by)"
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
