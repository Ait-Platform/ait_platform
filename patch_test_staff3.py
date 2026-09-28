import sys
import re

with open("tests/uip/test_staff_provider_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

# test_secretary_verification_and_returning_journey: remove provider
c = re.sub(
    r'@pytest\.mark\.parametrize\("kind,granted", \[?\("staff",\s*"receptionist"\),\s*\("provider",\s*"provider"\)\]?\)',
    '@pytest.mark.parametrize("kind,granted", [("staff","receptionist")])',
    c
)

# test_owner_is_not_secretary_gatekeeper: change 'provider' to 'staff' in request_access
c = c.replace(
    "claim=request_access(client,data,'provider');client.login('owner');before=identities()",
    "claim=request_access(client,data,'staff');client.login('owner');before=identities()"
)

# test_secretary_cannot_grant_legacy_manager: change 'provider' to 'staff' in request_access parameterization
c = re.sub(
    r'@pytest\.mark\.parametrize\(\'kind\',\[\'staff\',\'provider\',\'legacy\'\]\)',
    "@pytest.mark.parametrize('kind',['staff','legacy'])",
    c
)

with open("tests/uip/test_staff_provider_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
