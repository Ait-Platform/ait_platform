import sys
with open("tests/uip/test_staff_provider_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    "assert response.status_code==302 and response.location.endswith('/work-orders')",
    "assert response.status_code==302 and response.location.endswith('/provider-dashboard')"
)

with open("tests/uip/test_staff_provider_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
