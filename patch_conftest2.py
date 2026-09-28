import sys
import re
with open("tests/uip/conftest.py", "r", encoding="utf-8") as f:
    c = f.read()

# Just fix the specific broken part
c = c.replace("def migrate_phase54(connection):\n            migrate_phase55(connection):", "def migrate_phase54(connection):")

with open("tests/uip/conftest.py", "w", encoding="utf-8") as f:
    f.write(c)
