"""Account-linked meter exceptions on isolated PostgreSQL; no production writes."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_utilities_capture as fixture
class MeterExceptionTests(fixture.UtilitiesCaptureTests):
    exceptions_url='/meter_exceptions/26'
    def setUp(self):
        super().setUp()
        from app.program_billing import meter_exceptions
        mock=patch.object(meter_exceptions,'db',self.db);mock.start();self.addCleanup(mock.stop)
        self.exceptions=meter_exceptions
        self.session.execute(text("INSERT INTO bil_muni_account(id,property_id,account_number) VALUES(2,26,'OTHER-26'),(3,28,'FOREIGN-28')"))
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type,municipal_bill_number,status) VALUES(90,26,'OTHER-W','Water','OTHER-26','stolen'),(91,28,'FOREIGN-W','Water','FOREIGN-28','stolen')"))
        self.session.commit();self.conn.commit()
    def test_account_navigation_meters_and_owner_are_scoped(self):
        before=self.snapshot()
        accounts=self.client.get(self.accounts_url+'?view=list',headers=self.headers)
        document=html.fromstring(accounts.data)
        self.assertFalse(document.xpath('//tbody//a[text()="Meter Exceptions"]'))
        links=document.xpath('//a[text()="Meter Exceptions"]/@href')
        self.assertTrue(any('/meter_exceptions/26' in link and 'account_id=' not in link for link in links))
        page=self.client.get(self.exceptions_url+'?account_id=1',headers=self.headers)
        self.assertEqual(page.status_code,200,page.text);self.assertIn('MET-26',page.text);self.assertIn('Exception Register',page.text);self.assertNotIn('exception-create-form\"',page.text)
        self.assertNotIn('OTHER-W',page.text);self.assertNotIn('FOREIGN-W',page.text);self.assertIn('Back to Municipal Accounts',page.text)
        all_page=self.client.get(self.exceptions_url,headers=self.headers);self.assertIn('Select account',all_page.text);self.assertNotIn('OTHER-W',all_page.text)
        self.assertEqual(before,self.snapshot())
    def test_account_property_access_and_authentication(self):
        self.assertEqual(self.client.get(self.exceptions_url+'?account_id=3',headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(self.exceptions_url.replace('/26','/28'),headers=self.headers).status_code,403)
        self.assertEqual(self.client.get(self.exceptions_url).status_code,403)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(MeterExceptionTests(name) for name in MeterExceptionTests.__dict__ if name.startswith('test_'))
