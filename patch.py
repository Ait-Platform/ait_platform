import sys
with open("tests/uip/conftest.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace("migrate_phase11(connection)\n            migrate_phase54(connection)\n            current_request_schema(connection)", 
              "migrate_phase11(connection)\n            current_request_schema(connection)\n            migrate_phase54(connection)")

with open("tests/uip/conftest.py", "w", encoding="utf-8") as f:
    f.write(c)
