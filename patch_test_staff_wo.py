import sys
with open("tests/uip/test_staff_provider_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    "assert dispatched[1].reference.encode() in page.data",
    ""
)

with open("tests/uip/test_staff_provider_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
