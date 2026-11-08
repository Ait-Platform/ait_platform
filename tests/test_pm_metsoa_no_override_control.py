"""Remove obsolete form override without changing dated tariff calculation."""
import unittest
from unittest.mock import patch
from sqlalchemy import text
from lxml import html
import test_pm_municipal_metsoa as fixture

class MetsoaNoOverrideControlTests(unittest.TestCase):
    def setUp(self):
        self.fx=fixture.MunicipalMetsoaTests(methodName='test_permissions_and_token_free_pm_access_remain_unchanged')
        self.fx.setUp();self.addCleanup(self.fx.doCleanups);self.addCleanup(self.fx.tearDown)
    def assert_no_control(self,response):
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        self.assertFalse(tree.xpath('//input[@name="use_current_tariffs"]'))
        self.assertNotIn('Use currently available tariffs for historical calculation',response.text)
        return tree
    def test_selection_form_retains_month_recipient_and_property_context(self):
        tree=self.assert_no_control(self.fx.client.get('/billing/utilities/26/generate-metsoa',headers=self.fx.headers))
        self.assertTrue(tree.xpath('//input[@name="billing_month"]'))
        self.assertTrue(tree.xpath('//select[@name="recipient_id"]'))
        self.assertIn('property_id=26',tree.xpath('//a[contains(text(),"Back to Utilities")]/@href')[0])
    def test_generated_form_and_central_output_link_keep_context(self):
        tree=self.assert_no_control(self.fx.client.get(self.fx.generation_url+'&test_report=1&use_current_tariffs=1',headers=self.fx.headers))
        self.assertEqual(tree.xpath('//input[@name="billing_month"]/@value'),['2026-10'])
        self.assertEqual(tree.xpath('//select[@name="recipient_id"]/option[@selected]/@value'),['11'])
        links=tree.xpath('//a[@id="metsoa-output"]/@href')
        self.assertEqual(len(links),1)
        self.assertTrue(all('use_current_tariffs' not in link for link in links))
        self.assertTrue(all('billing_month=2026-10' in link and 'recipient_id=11' in link and 'test_report=1' in link for link in links))
    def test_stale_form_parameter_is_ignored_and_results_are_unchanged(self):
        results=[];original=self.fx.metsoa.build
        def build(*args,**kwargs):
            self.assertNotIn('use_current_tariffs',kwargs)
            data=original(*args,**kwargs);results.append(data);return data
        before=self.fx.snapshot();tariffs=list(self.fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id')))
        with patch.object(self.fx.metsoa,'build',side_effect=build):
            self.assert_no_control(self.fx.client.get(self.fx.generation_url,headers=self.fx.headers))
            self.assert_no_control(self.fx.client.get(self.fx.generation_url+'&use_current_tariffs=1',headers=self.fx.headers))
        first,second=results
        for key in ('grand_total','electricity','water','pm_status','pm_issues'):self.assertEqual(first[key],second[key])
        self.assertEqual([(r.id,r.rate,r.effective_date) for r in first['pm_tariff_records']],[(r.id,r.rate,r.effective_date) for r in second['pm_tariff_records']])
        self.assertEqual(before,self.fx.snapshot());self.assertEqual(tariffs,list(self.fx.session.execute(text('SELECT * FROM bil_tariff ORDER BY id'))))
    def test_missing_dated_rates_remain_unavailable_despite_old_query_parameter(self):
        self.fx.session.execute(text("UPDATE bil_tariff SET effective_date='2026-11-01'"))
        self.fx.session.commit();self.fx.conn.commit()
        response=self.fx.client.get(self.fx.generation_url+'&use_current_tariffs=1',headers=self.fx.headers)
        self.assert_no_control(response)
        self.assertIn('Rate unavailable',response.text)
        self.assertIn('INCOMPLETE',response.text)
        self.assertNotIn('Estimated charges',response.text)
    def test_existing_property_and_recipient_permissions_remain(self):
        self.assertEqual(self.fx.client.get(self.fx.generation_url.replace('/26/','/28/'),headers=self.fx.headers).status_code,403)
        self.assertEqual(self.fx.client.get(self.fx.generation_url.replace('recipient_id=11','recipient_id=999999'),headers=self.fx.headers).status_code,403)

if __name__=='__main__':unittest.main()
