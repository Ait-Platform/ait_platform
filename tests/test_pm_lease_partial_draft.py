"""Partial lease-period drafts save; strict generation-period validation remains."""
import unittest
from lxml import html
import test_pm_lease_period_offline as fixture

class PartialDraftTests(fixture.PeriodOfflineTests):
    def test_partial_period_saves_without_inventing_expiry(self):
        response=self.client.post(self.route,data={'draft_id':'synthetic-draft','version':'1','period_form_version':'2','s1-17':'12','s1-18':''})
        self.assertEqual(response.status_code,302,response.text)
        saved=self.saved
        self.assertEqual(saved['s1-17']['value'],'12 months')
        self.assertEqual(saved['s1-18']['value'],'')
        self.assertEqual(saved['s1-20']['value'],'')
        self.assertTrue(self.m.period_alerts(saved,require_complete=True))
        self.draft['items']=saved
        page=self.client.get(self.route)
        self.assertFalse(html.fromstring(page.data).xpath('//input[@name="s1-17" or @name="s1-18"][@required]'))

    def test_partial_period_rejects_malformed_supplied_values(self):
        for fields in ({'s1-17':'-1','s1-18':''},{'s1-17':'','s1-18':'not-a-date'}):
            response=self.client.post(self.route,data=dict(draft_id='synthetic-draft',version='1',period_form_version='2',**fields))
            self.assertEqual(response.status_code,400,response.text)
            self.assertIsNone(self.saved)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(PartialDraftTests(n) for n in PartialDraftTests.__dict__ if n.startswith('test_'))
