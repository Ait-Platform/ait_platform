import sys
from app import create_app
from app.extensions import db
from app.models.core import CoreOrganization, CoreOrganizationMember, CoreRole, CoreRoleAssignment
from app.models.auth import User

app = create_app()

def test_routes():
    with app.test_client() as client:
        with app.app_context():
            org = CoreOrganization.query.filter_by(slug='manor-gardens').first()
            if not org:
                print('Org not found')
                return
            
            # create test user
            user = User(name='Test User', email='test@example.com', is_active=True)
            db.session.add(user)
            db.session.commit()
            
            # log in
            with client.session_transaction() as sess:
                sess['_user_id'] = str(user.id)
                sess['_fresh'] = True
            
            # 1. Unverified participant tries to reach privileged dashboard -> 403
            resp = client.get(f'/uip/{org.slug}/subcommittee-dashboard')
            print('Direct privileged URL access without membership:', resp.status_code)
            assert resp.status_code == 403
            
            # 2. Add membership
            membership = CoreOrganizationMember(organization_id=org.id, user_id=user.id, is_active=True)
            db.session.add(membership)
            db.session.commit()
            
            # 3. Direct privileged URL access without role -> 403
            resp = client.get(f'/uip/{org.slug}/subcommittee-dashboard')
            print('Direct privileged URL access with membership but no role:', resp.status_code)
            assert resp.status_code == 403
            
            # 4. Route selection cannot self-grant authority (hitting verification endpoints)
            resp = client.get(f'/uip/{org.slug}/verify/subcommittee')
            print('Unverified verify_subcommittee:', resp.status_code, 'Content contains Verification pending?', b'awaiting verification' in resp.data)
            assert b'awaiting verification' in resp.data
            
            # 5. Route selection cannot self-grant authority
            resp = client.get(f'/uip/{org.slug}/verify/mo')
            assert b'awaiting verification' in resp.data
            
            # 6. Verify Public route works and no membership check blocks it
            resp = client.get(f'/uip/{org.slug}/verify/public')
            print('Verify public route:', resp.status_code)
            assert resp.status_code in [200, 302]
            
            # 7. Add role and test Manager Command Centre
            role = CoreRole.query.filter_by(slug='manager').first()
            assignment = CoreRoleAssignment(organization_id=org.id, user_id=user.id, role_id=role.id)
            db.session.add(assignment)
            db.session.commit()
            
            resp = client.get(f'/uip/{org.slug}/dashboard') # Should route manager
            print('Manager dashboard access:', resp.status_code, 'Content contains Manager Command Centre?', b'Manager Command Centre' in resp.data)
            assert b'Manager Command Centre' in resp.data
            
            # Cleanup
            db.session.delete(assignment)
            db.session.delete(membership)
            db.session.delete(user)
            db.session.commit()
            print('All tests passed successfully.')

if __name__ == '__main__':
    test_routes()
