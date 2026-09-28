import sys
import re
with open("app/program_uip/services/operational_admission.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(
    r'if claim\.category == "UIP_PROVIDER_ACCESS" or claim\.description == "Requested operational journey: provider":\n        return \("provider",\)\n    ',
    '', c
)
c = c.replace(
    'return ("receptionist", "provider")',
    'return ("receptionist",)'
)

with open("app/program_uip/services/operational_admission.py", "w", encoding="utf-8") as f:
    f.write(c)
