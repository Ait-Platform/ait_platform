import sys
with open("tests/uip/test_subcommittees.py", "r", encoding="utf-8") as f:
    c = f.read()

def patch_assert(c, find, error_msg):
    c = c.replace(f'assert "{error_msg}" in resp.get_data(as_text=True)', 
                  f'resp = client.get(BASE + "/secretary/organogram")\n    assert "{error_msg}" in resp.get_data(as_text=True)')
    return c

c = patch_assert(c, 'A Subcommittee must be established by a same-organisation ADOPTED resolution', 'A Subcommittee must be established by a same-organisation ADOPTED resolution')
c = patch_assert(c, 'same-organisation ADOPTED resolution', 'same-organisation ADOPTED resolution')
c = patch_assert(c, 'seats must be valid and belong to this organisation', 'seats must be valid and belong to this organisation')
c = patch_assert(c, "An active subcommittee named 'Duplicate Sub' already exists", "An active subcommittee named &#39;Duplicate Sub&#39; already exists")

with open("tests/uip/test_subcommittees.py", "w", encoding="utf-8") as f:
    f.write(c)
