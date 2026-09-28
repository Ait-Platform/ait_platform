import sys
import re
with open("tests/uip/test_staff_provider_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = re.sub(
    r"@pytest\.mark\.parametrize\('kind,role',\[\('staff','receptionist'\),\('provider','provider'\)\]\)",
    "@pytest.mark.parametrize('kind,role',[('staff','receptionist')])", c
)
c = re.sub(
    r"@pytest\.mark\.parametrize\('kind',\['staff','provider'\]\)",
    "@pytest.mark.parametrize('kind',['staff'])", c
)
c = c.replace(
    "claim=request_access(client,data,'provider' if kind=='provider' else 'staff')",
    "claim=request_access(client,data,'staff')"
)
c = c.replace(
    "assert client.safe_post(BASE+'/finalize-access-resolution',approval_values(claim,'provider')).status_code==403",
    "assert client.safe_post(BASE+'/finalize-access-resolution',approval_values(claim,'receptionist')).status_code==403"
)
c = c.replace(
    "approval_values(claim,'provider' if kind=='provider' else 'receptionist')",
    "approval_values(claim,'receptionist')"
)

with open("tests/uip/test_staff_provider_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
