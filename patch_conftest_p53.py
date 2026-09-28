import sys
import re
with open("tests/uip/conftest.py", "r", encoding="utf-8") as f:
    c = f.read()

# Add migrate_phase53
p53_code = """def migrate_phase53(connection):
    spec = importlib.util.spec_from_file_location("uip_phase53_revision", ROOT / "migrations/versions/uip_p53_proposals.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with Operations.context(MigrationContext.configure(connection)):
        module.upgrade()

def migrate_phase54(connection):"""

c = c.replace("def migrate_phase54(connection):", p53_code)
c = c.replace("current_request_schema(connection)\n            migrate_phase54(connection)\n            migrate_phase55(connection)", 
              "current_request_schema(connection)\n            migrate_phase53(connection)\n            migrate_phase54(connection)\n            migrate_phase55(connection)")

with open("tests/uip/conftest.py", "w", encoding="utf-8") as f:
    f.write(c)
