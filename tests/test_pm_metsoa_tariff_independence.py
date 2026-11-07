"""Utility-specific effective dates with isolated PostgreSQL fixtures."""
import unittest
from sqlalchemy import text
import test_pm_municipal_metsoa as fixture

class TariffIndependenceTests(fixture.MunicipalMetsoaTests):
    def test_water_change_does_not_omit_electricity(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-10-15' WHERE utility_type='water'"))
        self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertEqual(len(data['electricity']['rows']),2)
        self.assertFalse(data['pm_complete'])
        self.assertTrue(any('CEL888: Rate unavailable' in s for s in data['pm_issues']))

    def test_electricity_change_does_not_omit_water(self):
        self.session.execute(text("UPDATE bil_tariff SET effective_date='2026-10-15' WHERE utility_type='electricity'"))
        self.session.commit();self.conn.commit()
        with self.app.test_request_context():data=self.metsoa.build(26,'2026-10',recipient_id=11)
        self.assertEqual(len([w for w in data['water']['meters'] if not w.get('consumption_unavailable')]),2)
        self.assertEqual(data['electricity']['rows'],[])
        self.assertFalse(data['pm_complete'])
        page=self.client.get(self.generation_url,headers=self.headers)
        self.assertIn('INCOMPLETE',page.text)
        self.assertIn('Final amount due unavailable',page.text)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(TariffIndependenceTests(n) for n in TariffIndependenceTests.__dict__ if n.startswith('test_'))
