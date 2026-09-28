import sys
with open("tests/uip/test_subcomm_tools.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'assert sub1.name in resp.get_data(as_text=True)',
    'assert sub1.name in resp.get_data(as_text=True), resp.get_data(as_text=True)'
)

with open("tests/uip/test_subcomm_tools.py", "w", encoding="utf-8") as f:
    f.write(c)
