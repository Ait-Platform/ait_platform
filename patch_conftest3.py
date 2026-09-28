import sys
with open("tests/uip/conftest.py", "r", encoding="utf-8") as f:
    c = f.read()

func = """def migrate_phase57(connection):
    spec = importlib.util.spec_from_file_location("uip_phase57_revision", ROOT / "migrations/versions/uip_p57_provider_registration.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    from alembic.operations import Operations
    from alembic.migration import MigrationContext
    with Operations.context(MigrationContext.configure(connection)):
        module.upgrade()

"""

c = c.replace("def migrate_phase55(connection):", func + "def migrate_phase55(connection):")
c = c.replace("            migrate_phase55(connection)", "            migrate_phase55(connection)\n            migrate_phase57(connection)")

with open("tests/uip/conftest.py", "w", encoding="utf-8") as f:
    f.write(c)
