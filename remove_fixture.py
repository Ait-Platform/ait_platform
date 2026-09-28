import sys
import re
with open("tests/uip/test_proposal_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(r"@pytest\.fixture\(scope=.module., autouse=True\)\ndef proposal_schema\(engine\):.*?yield.*?revision\.downgrade\(\)\n", "", c, flags=re.DOTALL)

with open("tests/uip/test_proposal_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
