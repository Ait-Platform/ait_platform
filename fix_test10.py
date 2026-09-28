import sys
import re
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'Motivation="Motiv"\n        )',
    'motivation="Motiv"\n        ))'
)
c = c.replace(
    'motivation="Motiv"\n        )',
    'motivation="Motiv"\n        ))'
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
