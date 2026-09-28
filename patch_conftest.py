import sys
import re
with open("tests/uip/conftest.py", "r", encoding="utf-8") as f:
    c = f.read()

# I want to make sure it looks like this:
"""
def migrate_phase55(connection):
    spec = importlib.util.spec_from_file_location("uip_phase55_revision", ROOT / "migrations/versions/uip_p55_sub_tools.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with Operations.context(MigrationContext.configure(connection)):
        module.upgrade()

def migrate_phase54(connection):
"""

c = re.sub(r"def migrate_phase55.*?def migrate_phase54\(connection\)[^\n]*", 
"""def migrate_phase55(connection):
    spec = importlib.util.spec_from_file_location("uip_phase55_revision", ROOT / "migrations/versions/uip_p55_sub_tools.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with Operations.context(MigrationContext.configure(connection)):
        module.upgrade()

def migrate_phase54(connection):""", c, flags=re.DOTALL)

with open("tests/uip/conftest.py", "w", encoding="utf-8") as f:
    f.write(c)
