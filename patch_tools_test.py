import sys
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace("from tests.uip.test_subcommittees import make_resolution", "")

make_res = """
def make_resolution(org_id, user_id, desc, status):
    res = UipResolution(organization_id=org_id, meeting_id=None, survey_id=None, recorded_by=user_id, 
                        resolution_type="OPERATIONAL", description=desc, status=status)
    db.session.add(res)
    db.session.flush()
    return res
"""

c = make_res + "\n" + c

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
