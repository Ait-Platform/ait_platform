import sys

with open("migrations/versions/uip_p57_provider_registration.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace("down_revision = 'uip_p56'", "down_revision = 'uip_p56_submembership'")

with open("migrations/versions/uip_p57_provider_registration.py", "w", encoding="utf-8") as f:
    f.write(c)
