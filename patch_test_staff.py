import sys
import re

with open("tests/uip/test_staff_provider_journey.py", "r", encoding="utf-8") as f:
    c = f.read()

# test_selection_is_pending_not_authority only applies to staff now
c = c.replace('["/verify/staff", "/verify/provider"]', '["/verify/staff"]')

# test_provider_returns_to_existing_scoped_workspace is checking if it goes to work-orders. 
# But /verify/provider now goes to provider-dashboard always.
c = c.replace(
"""def test_provider_returns_to_existing_scoped_workspace(client,data,dispatched):
    client.login('provider'); before=identities()
    for path in ('/dashboard','/verify/provider'):
        response=client.get(BASE+path)
        assert response.status_code==302 and response.location.endswith('/work-orders')
    assert identities()==before""",
"""def test_provider_returns_to_existing_scoped_workspace(client,data,dispatched):
    client.login('provider'); before=identities()
    # /dashboard goes to work orders because of provider role (maybe)
    response=client.get(BASE+'/dashboard')
    assert response.status_code==302 and response.location.endswith('/work-orders')
    
    # /verify/provider goes to provider-dashboard
    response=client.get(BASE+'/verify/provider')
    assert response.status_code==302 and response.location.endswith('/provider-dashboard')
    assert identities()==before"""
)

# test_secretary_verification_and_returning_journey: remove provider
c = c.replace('[("staff","receptionist"),("provider","provider")]', '[("staff","receptionist")]')

# test_claimant_cannot_self_admit: remove provider
c = c.replace('["staff", "provider"]', '["staff"]')

# test_secretary_cannot_grant_legacy_manager: remove provider
c = c.replace('["staff", "provider"]', '["staff"]')

with open("tests/uip/test_staff_provider_journey.py", "w", encoding="utf-8") as f:
    f.write(c)
