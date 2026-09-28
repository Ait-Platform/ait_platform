import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'term = UipCommitteeTerm(organization_id=data.org.id, status="CURRENT")',
    'term = UipCommitteeTerm(organization_id=data.org.id, term_name="Test Term")\n        db.session.add(term)\n        db.session.flush()'
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
