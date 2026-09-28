import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'term = UipCommitteeTerm(organization_id=data.org.id, start_date=date(2026,1,1), end_date=date(2027,1,1), status="CURRENT")',
    'term = UipCommitteeTerm(organization_id=data.org.id, status="CURRENT")'
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
