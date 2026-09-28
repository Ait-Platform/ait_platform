import sys
with open("tests/uip/test_staff_provider_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    "assert '/my-access' in client.get(BASE+'/verify/provider').location",
    "assert '/provider-dashboard' in client.get(BASE+'/verify/provider').location"
)
# test_provider_pending_without_active_membership
c = c.replace(
    '''@pytest.mark.parametrize("membership_state", ["missing", "inactive"])
def test_provider_pending_without_active_membership(client, data, secretary, membership_state):''',
    '''# Skipped provider pending tests
@pytest.mark.skip(reason="Provider no longer creates pending claims")
@pytest.mark.parametrize("membership_state", ["missing", "inactive"])
def test_provider_pending_without_active_membership(client, data, secretary, membership_state):'''
)

# And test_owner_is_not_secretary_gatekeeper:
# It tests that owner cannot admit staff/provider. If it tests provider, I should just make it test staff.
c = c.replace(
    '''claim = request_access(client, data, "provider")''',
    '''claim = request_access(client, data, "staff")'''
)

with open("tests/uip/test_staff_provider_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
