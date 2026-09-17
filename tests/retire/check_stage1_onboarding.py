"""Focused database-free Stage 1 request/security regression."""
import unittest
from unittest.mock import patch
from flask_login import AnonymousUserMixin
from check_stage2 import Stage2Tests, Organisation, Membership, Query, routes


class Stage1Tests(unittest.TestCase):
    setUp = Stage2Tests.setUp
    tearDown = Stage2Tests.tearDown

    def test_about_leads_to_fork(self):
        self.assertIn(b'/retire/onboarding', self.client.get('/retire/about').data)
        page = self.client.get('/retire/onboarding')
        self.assertEqual(page.status_code, 200)
        self.assertIn(b'Owner / Organisation Administrator', page.data)
        self.assertIn(b'/retire/other', page.data)

    def test_other_redirects_to_waiting_without_creating_home(self):
        response = self.client.get('/retire/other')
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.location.endswith('/retire/waiting-room'))
        self.assertEqual(self.client.post('/retire/other').status_code, 405)
        self.session.add.assert_not_called()
        self.session.commit.assert_not_called()

    def test_declaration_initially_unchecked(self):
        import re
        body = self.client.get('/retire/register').get_data(as_text=True)
        field = re.search(r'<input[^>]*name="authority"[^>]*>', body).group()
        self.assertNotIn('checked', field)

    def test_missing_or_false_declaration_writes_nothing(self):
        for value in (None, '', 'false'):
            data = {'name':'New home'}
            if value is not None:
                data['authority'] = value
            self.assertEqual(self.client.post('/retire/register', data=data).status_code, 200)
        self.session.add.assert_not_called()
        self.session.commit.assert_not_called()

    def test_existing_founder_can_create_and_client_cannot_set_authority(self):
        response = self.client.post('/retire/register', data={
            'name':self.org.name, 'authority':'y', 'owner_user_id':999,
            'user_id':999, 'approved_role_id':5, 'organisation_id':1})
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.location.endswith('/organisations/3/setup'))
        home, member = [c.args[0] for c in self.session.add.call_args_list]
        self.assertIsNot(home, self.org)
        self.assertEqual(home.owner_user_id, 10)
        self.assertEqual((member.organisation_id,member.user_id,member.approved_role_id),(3,10,1))

    def test_anonymous_cannot_create(self):
        with patch.object(routes, 'current_user', AnonymousUserMixin()):
            # login_required resolves Flask-Login's user separately.
            self.app.login_manager._request_callback = lambda request: None
            response = self.client.post('/retire/register', data={'name':'New','authority':'y'})
        self.assertEqual(response.status_code, 401)
        self.session.add.assert_not_called()

    def test_setup_is_owner_only_and_has_no_operational_links(self):
        response = self.client.get('/retire/organisations/1/setup')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'activation will be completed in the next setup stage', response.data)
        self.assertNotIn(b'/members/pending', response.data)
        self.org.owner_user_id = 99
        denied = self.client.get('/retire/organisations/1/setup')
        self.assertEqual(denied.status_code, 302)
        self.assertTrue(denied.location.endswith('/organisations/1/status'))
        self.assertEqual(self.client.get('/retire/organisations/2/setup').status_code, 403)

    def test_existing_operations_not_globally_blocked(self):
        self.assertEqual(self.client.get('/retire/organisations/1/dashboard').status_code, 200)
        self.assertEqual(self.client.get('/retire/organisations/1/members/pending').status_code, 200)


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromTestCase(Stage1Tests)


if __name__ == '__main__':
    unittest.main()
