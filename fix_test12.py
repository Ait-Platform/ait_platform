import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'title="Sub Proposal", description="Desc", motivation="Motiv"\n    )\n    \n    prop',
    'title="Sub Proposal", description="Desc", motivation="Motiv"\n    ))\n    \n    prop'
)

c = c.replace(
    'title="Finance Proposal", description="Desc", motivation="Motiv"\n    )\n    \n    prop',
    'title="Finance Proposal", description="Desc", motivation="Motiv"\n    ))\n    \n    prop'
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
