import sys
import re

with open("tests/uip/test_staff_provider_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(
    r"for path in \('/dashboard','/verify/provider'\):\n        response=client\.get\(BASE\+path\)\n        assert response\.status_code==302 and response\.location\.endswith\('/work-orders'\)",
    "response=client.get(BASE+'/dashboard')\n    assert response.status_code==302 and response.location.endswith('/work-orders')\n    response=client.get(BASE+'/verify/provider')\n    assert response.status_code==302 and response.location.endswith('/provider-dashboard')",
    c
)

with open("tests/uip/test_staff_provider_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
