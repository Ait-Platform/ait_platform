"""Focused Special Conditions save/reopen using temporary local PostgreSQL fixtures."""
import copy,sys,types,unittest
from unittest.mock import patch
from lxml import html
import test_pm_lease_lessee_persistence as fixtures

class SpecialConditionsPersistenceTests(unittest.TestCase):
    stored=fixtures.LesseePersistenceTests.stored
    def setUp(self):
        fixtures.LesseePersistenceTests.setUp(self)
        standard=types.ModuleType('app.program_billing.lease_standard')
        fixtures.functions('app/program_billing/lease_standard.py',{'special_conditions'},standard.__dict__)
        self.legacy=patch.dict(sys.modules,{standard.__name__:standard});self.legacy.start();self.addCleanup(self.legacy.stop)
        self.url=self.url.replace('/parties?','/additional?')
    def payload(self,clauses):
        data={'csrf_token':self.csrf,'version':str(self.stored()['version']),'action':'save','sc_editor_version':'1','sc_mode':'add','sc_count':str(len(clauses))}
        for index,(title,body) in enumerate(clauses,1):
            data.update({'sc_heading_type_'+str(index):'Custom','sc_custom_heading_'+str(index):title,'sc_text_'+str(index):body})
        return data
    def test_save_add_edit_reopen_and_preserve_other_particulars(self):
        before=copy.deepcopy(self.stored()['items'])
        clauses=[('Original Mixed Heading','Keep Body Case.')]
        for clauses in (clauses,clauses+[('Second Heading','Second Body.')],[('Edited Heading','Edited Body.'),('Second Heading','Second Body.')]):
            response=self.client.post(self.url,headers={'X-Test-Owner':'yes'},data=self.payload(clauses))
            self.assertEqual(response.status_code,302)
            saved=self.stored()['items'];self.assertEqual([(row['heading'],row['text']) for row in saved['_supplementary_conditions']['clauses']],clauses)
            self.assertEqual({key:value for key,value in saved.items() if key!='_supplementary_conditions'},before)
            page=self.client.get(response.location,headers={'X-Test-Owner':'yes'});self.assertEqual(page.status_code,200)
            tree=html.fromstring(page.data);self.assertFalse(tree.xpath('//*[@id="capture-save"]'))
            self.assertTrue(tree.xpath('//*[@id="sc-entry"][@hidden][@disabled]'))
            self.assertEqual(len(tree.xpath('//*[@id="sc-save-clause"]')),1)
            for _,body in clauses:self.assertIn(body,page.text)
    def test_stale_unauthorized_and_csrf_saves_do_not_change_data(self):
        data=self.payload([('New Heading','New Body.')]);before=copy.deepcopy(self.stored()['items'])
        for headers,payload in (({},data),({'X-Test-Owner':'yes'},{**data,'csrf_token':''}),({'X-Test-Owner':'yes'},{**data,'version':'0'})):
            response=self.client.post(self.url,headers=headers,data=payload)
            self.assertIn(response.status_code,(400,403,409));self.assertEqual(self.stored()['items'],before)

if __name__=='__main__':unittest.main()
