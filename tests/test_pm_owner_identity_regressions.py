"""Real property creation and exception identities stay independent per manager."""
import unittest
from sqlalchemy import text
from test_pm_property_management import PropertyManagementTests
from test_pm_meter_exception_historical import HistoricalExceptionTests

class PropertyNamesTests(PropertyManagementTests):
    def test_two_authenticated_users_create_almond_independently(self):
        self.session.execute(text("UPDATE bil_property SET name='Earlier development property' WHERE id=26"));self.session.commit();self.conn.commit()
        for user in (7,8):
            self.sign_in(user)
            response=self.client.post('/billing/property-owner',data={'property_name':'Almond'})
            self.assertEqual(response.status_code,302,response.text)
            page=self.client.get('/billing/property-owner')
            self.assertEqual(page.status_code,200)
        self.assertEqual(list(self.session.execute(text("SELECT manager_id,name FROM bil_property WHERE name='Almond' ORDER BY manager_id"))),[(7,'Almond'),(8,'Almond')])

class ExceptionIdentityUsersTests(HistoricalExceptionTests):
    def test_both_users_register_same_historical_identity_without_cross_account_conflict(self):
        for user,p,account in [(7,26,'1'),(8,28,'3')]:
            self.user.id=user;self.exceptions_url='/meter_exceptions/'+str(p)
            response=self.post_exception(self.historical_fields(account_id=account))
            self.assertEqual(response.status_code,302,response.text)
        rows=list(self.session.execute(text('SELECT property_id,meter_number,identity_key FROM bil_meter_exception_identity ORDER BY property_id')))
        self.assertEqual([r.property_id for r in rows],[26,28]);self.assertEqual(rows[0].meter_number,rows[1].meter_number);self.assertNotEqual(rows[0].identity_key,rows[1].identity_key)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(cls(name) for cls in (PropertyNamesTests,ExceptionIdentityUsersTests) for name in cls.__dict__ if name.startswith('test_'))
