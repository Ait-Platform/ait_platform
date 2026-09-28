import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(r"motivation=\"Motiv\"\n        \)", "motivation=\"Motiv\"\n        ))", c)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
